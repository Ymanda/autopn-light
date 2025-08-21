#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_build_refs_and_master_csv.py
But:
- Lire un CSV "events" (sortie de la détection des sophismes) où chaque ligne = 1 argument (occurrence) dans 1 email.
- MERGER les FAITS par fact_ref (phrase canonique) et les ARGUMENTS par arg_ref (phrase canonique).
- Appliquer tes règles "veracity" 75/25 (100 si locked).
- Calculer strength = veracity * relevance * reasoning_credibility * impact_score (PAS de pénalité sophisme).
- Classer les sujets open/hidden à partir des hints.
- Produire:
  - arguments_master.csv
  - facts_master.csv
  - topics_master.csv (mis à jour)
  - topics_rollup.csv (score Maryvonne vs Yannick + gagnant)

Entrée attendue (events.csv) colonnes (souples):
  - email_id (str), date_iso (ISO)
  - speaker_name, speaker_email
  - topic_name (str), topic_side (pro|con)
  - argument_text (str), argument_ref_name (str|optional)
  - reasoning_name (str|optional)
  - sophism_name (str|optional), sophism_category (str|optional)
  - fact_texts (str | "A || B || C")
  - hidden_topic_hint (str|optional), hidden_topic_confidence (float in [0,1]|optional)
  - relevance, reasoning_credibility, impact_score (float from set {0.01,0.05,0.10,0.25,0.50,0.60,0.75,0.90,1.00})
  - impact_direction (positive|negative)

Optionnel: fichier seed de faits "facts_seed.csv" (pour locked=1) avec colonnes:
  - canonical_text, fact_ref_id, locked (0|1)

Usage:
  python3 03_build_refs_and_master_csv.py --events events_sample.csv --outdir out/ \
      --user-emails y.mandaba@gmail.com mymmandaba@gmail.com \
      --relation-emails mmymmandaba@gmail.com maryvonnemm@gmail.com maryvonne.masseron@free.fr maryvonnemm@hotmail.com maryvonnemm@hotmail.fr

"""

import csv
import os
import sys
import re
import argparse
import hashlib
import datetime
from collections import defaultdict, Counter

NOW_ISO = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# --------- Utilitaires ---------
def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def slugish(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^\w\s-]+", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s or "na"

def hash_id(prefix: str, s: str, n=8) -> str:
    h = hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:n]
    return f"{prefix}-{h}"

def is_floaty(x):
    try:
        float(x)
        return True
    except:
        return False

def to_bucket(x):
    """Force une valeur dans {0.01,0.05,0.10,0.25,0.50,0.60,0.75,0.90,1.00} si x n'y est pas."""
    allowed = [0.01,0.05,0.10,0.25,0.50,0.60,0.75,0.90,1.00]
    if x in allowed:
        return x
    # Approche simple: arrondi au plus proche
    return min(allowed, key=lambda a: abs(a - x))

def to_float_or_default(x, dflt):
    if is_floaty(x):
        return float(x)
    return dflt

def split_facts(raw: str):
    """Supporte '||' ou '|' ou ';' comme séparateurs."""
    if raw is None:
        return []
    txt = raw.replace("||", "|")
    parts = [norm_space(p) for p in re.split(r"[|;]", txt) if norm_space(p)]
    return parts

def pick_arg_ref_name(argument_ref_name: str, argument_text: str) -> str:
    if norm_space(argument_ref_name):
        return norm_space(argument_ref_name)
    # Sinon, derive une phrase courte depuis argument_text
    t = norm_space(argument_text)
    # couper à ~120 chars à la fin d'un mot/phrase
    if len(t) > 120:
        cut = t[:120].rfind(" ")
        if cut < 60:  # sécurité
            cut = 120
        t = t[:cut] + "…"
    return t or "(argument)"

def canonical_fact(text: str) -> str:
    """Phrase canonique = texte normalisé léger (on laisse la sémantique intacte).
       Tu pourras brancher une IA de paraphrase plus tard."""
    return norm_space(text)

def derive_topic_id(topic_name: str) -> str:
    base = slugish(norm_space(topic_name))
    return f"TOP-{base[:32]}" if base!="na" else "TOP-NA"

def derive_speaker_role(speaker_email: str, user_emails, relation_emails) -> str:
    e = (speaker_email or "").lower()
    if e in user_emails:
        return "User"
    if e in relation_emails:
        return "Relation"
    return "Other"

def veracity_from_status_pct(status_pct: int) -> float:
    mapv = {0:0.0, 25:0.25, 50:0.50, 75:0.75, 100:1.00}
    return mapv.get(int(status_pct), 0.0)

# --------- Lecture seed de faits (locked) ---------
def load_seed_facts(path):
    seed = {}  # canonical_text -> (fact_ref_id, locked)
    if not path or not os.path.exists(path):
        return seed
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ct = canonical_fact(row.get("canonical_text",""))
            if not ct:
                continue
            fr = row.get("fact_ref_id") or hash_id("FREF", ct)
            locked = 1 if str(row.get("locked","0")).strip() in ("1","true","True") else 0
            seed[ct] = (fr, locked)
    return seed


# ---------- TUNING HELPERS (add near imports) ----------
def _read_tuning_csv(path, key_field):
    import csv, os
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            k = (row.get(key_field) or "").strip()
            if not k:
                continue
            out[k] = {
                "manual_strength": (row.get("manual_strength") or "").strip(),
                "multiplier": (row.get("multiplier") or "").strip(),
                "notes": (row.get("notes") or "").strip(),
            }
    return out

def _to_float(s, default=1.0):
    try:
        return float(s)
    except Exception:
        return default

def _clamp01(x):
    return max(0.0, min(1.0, float(x)))

def _apply_tuning(base_strength, ref_id, tuning_map):
    t = tuning_map.get(ref_id)
    if not t:
        return _clamp01(base_strength)
    if t.get("manual_strength"):
        return _clamp01(_to_float(t["manual_strength"], base_strength))
    mult = _to_float(t.get("multiplier"), 1.0)
    return _clamp01(base_strength * mult)

# --------- Pipeline ---------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="CSV des événements (sortie sophismes)")
    ap.add_argument("--outdir", required=True, help="Dossier de sortie")
    ap.add_argument("--facts-seed", default="", help="CSV seed de faits (canonical_text, fact_ref_id, locked)")
    ap.add_argument("--user-emails", nargs="*", default=["y.mandaba@gmail.com","mymmandaba@gmail.com"], help="Emails 'User'")
    ap.add_argument("--relation-emails", nargs="*", default=["maryvonnemm@gmail.com","maryvonne.masseron@free.fr","maryvonnemm@hotmail.com","maryvonnemm@hotmail.fr"], help="Emails 'Relation'")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Chargement seed facts (locked)
    seed = load_seed_facts(args.facts_seed)

    # Lecture events
    rows = []
    with open(args.events, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    if not rows:
        print("❌ Aucun événement lu.")
        sys.exit(1)

    # Structures d'agrégation
    topic_meta = {}   # topic_id -> {topic_name, hidden_hints[], hidden_conf[], counts...}
    fact_groups = {}  # fact_ref_id -> dict about fact
    fact_index_by_text = {}  # canonical_text -> fact_ref_id (utilise seed si présent)
    arg_lines = []    # sorties arguments_master (lignes)
    # Trace pro/con par fact_ref_id
    fact_side_counter = defaultdict(lambda: Counter())  # fact_ref_id -> {"pro": n, "con": n}

    # 1) Première passe: expansion 1 argument x N faits => N lignes
    for i, ev in enumerate(rows, start=1):
        email_id = norm_space(ev.get("email_id"))
        date_iso = norm_space(ev.get("date_iso")) or NOW_ISO
        speaker_name  = norm_space(ev.get("speaker_name"))
        speaker_email = norm_space(ev.get("speaker_email")).lower()
        topic_name    = norm_space(ev.get("topic_name"))
        topic_side    = norm_space(ev.get("topic_side")).lower()  # pro|con
        argument_text = norm_space(ev.get("argument_text"))
        argument_ref_name = pick_arg_ref_name(ev.get("argument_ref_name",""), argument_text)
        reasoning_name = norm_space(ev.get("reasoning_name"))
        sophism_name = norm_space(ev.get("sophism_name"))
        sophism_category = norm_space(ev.get("sophism_category"))
        fact_texts_raw = ev.get("fact_texts","")
        hidden_topic_hint = norm_space(ev.get("hidden_topic_hint"))
        hidden_topic_confidence = to_float_or_default(ev.get("hidden_topic_confidence",""), 0.0)
        relevance = to_bucket(to_float_or_default(ev.get("relevance",""), 0.50))
        reasoning_credibility = to_bucket(to_float_or_default(ev.get("reasoning_credibility",""), 0.50))
        impact_score = to_bucket(to_float_or_default(ev.get("impact_score",""), 0.50))
        impact_direction = norm_space(ev.get("impact_direction")) or "positive"

        topic_id = derive_topic_id(topic_name)
        speaker_role = derive_speaker_role(speaker_email, set([e.lower() for e in args.user_emails]),
                                           set([e.lower() for e in args.relation_emails]))
        # arg_ref_id stable
        arg_ref_id = hash_id("ARREF", argument_ref_name, n=10)
        # arg_instance_id pour grouper les multiples faits cités dans CETTE occurrence d'argument
        arg_instance_id = hash_id("ARGI", f"{email_id}|{argument_ref_name}|{speaker_email}|{topic_id}|{topic_side}", n=10)

        # MàJ topic_meta (hints)
        tm = topic_meta.get(topic_id) or {
            "topic_name": topic_name,
            "hidden_hints": [],
            "hidden_conf": [],
        }
        if hidden_topic_hint:
            tm["hidden_hints"].append(hidden_topic_hint)
            tm["hidden_conf"].append(hidden_topic_confidence)
        topic_meta[topic_id] = tm

        facts_list = split_facts(fact_texts_raw)
        if not facts_list:
            # On autorise une ligne SANS fait: on rattache à un pseudo-fait minimal, pour ne pas perdre l’argument.
            facts_list = ["(fait non spécifié)"]

        for ftxt in facts_list:
            ctext = canonical_fact(ftxt)
            # Résoudre fact_ref_id depuis seed ou index interne
            if ctext in seed:
                frid, locked = seed[ctext]
            else:
                frid = fact_index_by_text.get(ctext)
                locked = 0
                if not frid:
                    frid = hash_id("FREF", ctext, n=10)
                    fact_index_by_text[ctext] = frid
            # Compter pro/con
            side = "pro" if topic_side == "pro" else "con"
            fact_side_counter[frid][side] += 1

            # Conserver info brute sur le groupe (mis à jour plus tard)
            fg = fact_groups.get(frid) or {
                "canonical_text": ctext,
                "locked": locked,
                "first_seen_email_id": email_id,
                "last_seen_email_id": email_id,
            }
            fg["last_seen_email_id"] = email_id
            fact_groups[frid] = fg

            # On pousse la ligne argument (veracity/status sera fixé en 2ème passe)
            arg_lines.append({
                "arg_line_id": "",  # on mettra après
                "arg_instance_id": arg_instance_id,
                "arg_ref_id": arg_ref_id,
                "arg_ref_name": argument_ref_name,
                "argument_text": argument_text,
                "email_id": email_id,
                "created_at": date_iso,
                "speaker_name": speaker_name,
                "speaker_email": speaker_email,
                "speaker_role": speaker_role,
                "topic_id": topic_id,
                "topic_side": "pro" if topic_side=="pro" else "con",
                "reasoning_name": reasoning_name,
                "sophism_name": sophism_name,
                "sophism_category": sophism_category,
                "fact_ref_id": frid,
                "fact_veracity_pct": None,  # à renseigner
                "relevance": relevance,
                "reasoning_credibility": reasoning_credibility,
                "impact_score": impact_score,
                "impact_direction": impact_direction if impact_direction in ("positive","negative") else "positive",
                "hidden_topic_hint": hidden_topic_hint,
                "hidden_topic_confidence": hidden_topic_confidence,
                "strength": None,   # à calculer
                "needs_review": 0,  # à marquer si contested
                "notes": ""
            })

    # 2) Statut des faits (75/25/100) selon règle
    facts_master_rows = []
    for frid, fg in fact_groups.items():
        locked = fg.get("locked", 0)
        pro_n = fact_side_counter[frid]["pro"]
        con_n = fact_side_counter[frid]["con"]
        if locked == 1:
            status_pct = 100
            status_source = "Human"
            status_method = "base_context"
            rationale = "Locked from base_context"
        else:
            if con_n > 0:
                status_pct = 25
                status_source = "AI"
                status_method = "contested"
                rationale = "Au moins un contre-argument observé"
            else:
                status_pct = 75
                status_source = "AI"
                status_method = "close_match"
                rationale = "Aucun contre-argument observé"

        facts_master_rows.append({
            "fact_ref_id": frid,
            "canonical_text": fg["canonical_text"],
            "status_pct": status_pct,
            "locked": locked,
            "status_source": status_source,
            "status_method": status_method,
            "rationale": rationale,
            "pro_occurrences": pro_n,
            "con_occurrences": con_n,
            "first_seen_email_id": fg["first_seen_email_id"],
            "last_seen_email_id": fg["last_seen_email_id"],
            "updated_at": NOW_ISO
        })

    # Index pour récupérer status_pct par fact_ref_id
    fact_status_pct = {row["fact_ref_id"]: int(row["status_pct"]) for row in facts_master_rows}
    needs_review_fact = {row["fact_ref_id"]: (row["status_method"]=="contested") for row in facts_master_rows}

    # 3) Compléter arguments_master (veracity & strength)
    for idx, line in enumerate(arg_lines, start=1):
        line["arg_line_id"] = f"ARG-L-{idx:07d}"
        pct = fact_status_pct.get(line["fact_ref_id"], 0)
        line["fact_veracity_pct"] = pct
        veracity = veracity_from_status_pct(pct)
        strength = veracity * float(line["relevance"]) * float(line["reasoning_credibility"]) * float(line["impact_score"])
        # rondeur
        line["strength"] = round(strength, 4)
        if needs_review_fact.get(line["fact_ref_id"], False):
            line["needs_review"] = 1




    # Optional tuning files (create them later if you want to tweak values)
    ARGREF_TUNING_PATH = os.path.join(args.outdir, "argref_tuning.csv")
    FACTREF_TUNING_PATH = os.path.join(args.outdir, "factref_tuning.csv")

    argref_tuning = _read_tuning_csv(ARGREF_TUNING_PATH, key_field="arg_ref_id")
    factref_tuning = _read_tuning_csv(FACTREF_TUNING_PATH, key_field="fact_ref_id")
    
    


        # 4) Topics aggregation (FULL tables, with tuning) + rollup + conclusions
    

    topics_master_rows = []
    topics_rollup_rows = []
    topics_argrefs_table_rows = []   # all arg_refs per topic/side
    topics_factrefs_table_rows = []  # all fact_refs per topic/side
    topics_conclusions_rows = []     # final 2-column summary per topic

    # Pretty maps
    argref_name = {}
    for l in arg_lines:
        argref_name[l["arg_ref_id"]] = l["arg_ref_name"]

    # Group per topic/side
    # For arguments: topic -> side -> arg_ref_id -> list[strengths]
    by_topic_side_argref = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # For facts: topic -> side -> fact_ref_id -> list[strengths]
    by_topic_side_factref = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    hidden_stat = defaultdict(lambda: {"vals": [], "notes": []})

    for line in arg_lines:
        t = line["topic_id"]
        side = "pro" if line["topic_side"] == "pro" else "con"
        ar = line["arg_ref_id"]
        st = float(line["strength"] or 0.0)
        by_topic_side_argref[t][side][ar].append(st)

        fr = (line.get("fact_ref_id") or "").strip()
        if fr:
            by_topic_side_factref[t][side][fr].append(st)

        if line.get("hidden_topic_hint"):
            hidden_stat[t]["vals"].append(float(line.get("hidden_topic_confidence") or 0.0))
            if line.get("hidden_topic_hint"):
                hidden_stat[t]["notes"].append(line["hidden_topic_hint"])

    def _visibility_for_topic(tid):
        vals = hidden_stat[tid]["vals"]
        notes = hidden_stat[tid]["notes"]
        open_ratio_hidden_hints = 0.0
        top_hidden_note = ""
        visibility = "open"
        if vals:
            sig = [v for v in vals if v >= 0.50]
            open_ratio_hidden_hints = round(len(sig) / max(1, len(vals)), 3)
            if open_ratio_hidden_hints >= 0.40:
                visibility = "hidden"
            if notes:
                c = Counter([n for n in notes if n])
                if c:
                    top_hidden_note = c.most_common(1)[0][0]
        return visibility, open_ratio_hidden_hints, top_hidden_note

    # Per-topic pass
    for topic_id in sorted(by_topic_side_argref.keys()):
        topic_name = topic_meta.get(topic_id, {}).get("topic_name", "(sujet)")
        visibility, open_ratio_hidden_hints, top_hidden_note = _visibility_for_topic(topic_id)

        # --- ARGUMENTS: build full table + tuned totals ---
        def side_args_summary(side_key):
            arg_map = by_topic_side_argref[topic_id].get(side_key, {})
            # base metrics per arg_ref
            # We'll use MAX as the family strength, but also keep count/sum/avg for reference
            per_argref = []
            tuned_total = 0.0
            base_total = 0.0
            for ar, vals in arg_map.items():
                cnt = len(vals)
                ssum = round(sum(vals), 6)
                smax = round(max(vals) if vals else 0.0, 6)
                savg = round((ssum / cnt) if cnt else 0.0, 6)
                tuned = _apply_tuning(smax, ar, argref_tuning)  # tune per arg_ref_id
                base_total += smax
                tuned_total += tuned

                topics_argrefs_table_rows.append({
                    "topic_id": topic_id,
                    "topic_name": topic_name,
                    "arg_ref_id": ar,
                    "arg_ref_name": argref_name.get(ar, "(argument)"),
                    "side": "pro" if side_key == "pro" else "con",
                    "count_occurrences": cnt,
                    "sum_strength": round(ssum, 4),
                    "avg_strength": round(savg, 4),
                    "max_strength": round(smax, 4),
                    "tuned_strength": round(tuned, 4),
                })
                per_argref.append((ar, argref_name.get(ar, "(argument)"), tuned))
            return round(base_total, 4), round(tuned_total, 4), per_argref

        # --- FACTS: build full table + tuned totals (for inspection; not used in net score directly) ---
        def side_facts_summary(side_key):
            fact_map = by_topic_side_factref[topic_id].get(side_key, {})
            per_factref = []
            for fr, vals in fact_map.items():
                cnt = len(vals)
                ssum = round(sum(vals), 6)
                smax = round(max(vals) if vals else 0.0, 6)
                savg = round((ssum / cnt) if cnt else 0.0, 6)
                tuned = _apply_tuning(smax, fr, factref_tuning)  # tune per fact_ref_id

                topics_factrefs_table_rows.append({
                    "topic_id": topic_id,
                    "topic_name": topic_name,
                    "fact_ref_id": fr,
                    "side": "pro" if side_key == "pro" else "con",
                    "count_occurrences": cnt,
                    "sum_strength": round(ssum, 4),
                    "avg_strength": round(savg, 4),
                    "max_strength": round(smax, 4),
                    "tuned_strength": round(tuned, 4),
                })
                per_factref.append((fr, tuned))
            return per_factref

        pro_base, pro_tuned, pro_arg_list = side_args_summary("pro")
        con_base, con_tuned, con_arg_list = side_args_summary("con")
        _ = side_facts_summary("pro")
        _ = side_facts_summary("con")

        # topics_master.csv (unchanged)
        topics_master_rows.append({
            "topic_id": topic_id,
            "topic_name": topic_name,
            "visibility": visibility,
            "hidden_notes": top_hidden_note,
            "owner_speaker": "",
            "created_at": NOW_ISO,
            "updated_at": NOW_ISO
        })

        # topics_rollup.csv (totals & counts; still useful)
        pro_uniq = len({ar for ar, _, _ in pro_arg_list})
        con_uniq = len({ar for ar, _, _ in con_arg_list})
        topics_rollup_rows.append({
            "topic_id": topic_id,
            "topic_name": topic_name,
            "visibility": visibility,
            "pro_total_strength": pro_base,
            "con_total_strength": con_base,
            "pro_total_strength_tuned": round(pro_tuned, 4),
            "con_total_strength_tuned": round(con_tuned, 4),
            "pro_unique_args": pro_uniq,
            "con_unique_args": con_uniq,
            "open_ratio_hidden_hints": open_ratio_hidden_hints,
            "top_hidden_note": top_hidden_note
        })

        # CONCLUSION row (no winner here, just the score; you decide later)
        # net score (tuned): PRO minus CON
        net_score = round(pro_tuned - con_tuned, 4)

        # pack full PRO/CON lists for your UI / spreadsheet filtering
        def pack_arglist(lst):
            # "arg_ref_id::arg_ref_name::tuned_strength" separated by " || "
            return " || ".join(f"{ar}::{nm}::{round(st,4)}" for ar, nm, st in sorted(lst, key=lambda x: x[2], reverse=True))

        topics_conclusions_rows.append({
            "topic_id": topic_id,
            "topic_name": topic_name,
            "visibility": visibility,
            "pro_total_tuned": round(pro_tuned, 4),
            "con_total_tuned": round(con_tuned, 4),
            "net_score": net_score,   # >0 → PROs weigh more; <0 → CONs weigh more
            "pro_argrefs": pack_arglist(pro_arg_list),
            "con_argrefs": pack_arglist(con_arg_list),
            "note_hidden": top_hidden_note
        })

    # 5) Write CSVs (+ HTML conclusion)
    def write_csv(path, fieldnames, rows):
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                out = {k: r.get(k, "") for k in fieldnames}
                w.writerow(out)

    write_csv(
        os.path.join(args.outdir, "facts_master.csv"),
        ["fact_ref_id","canonical_text","status_pct","locked","status_source","status_method","rationale",
         "pro_occurrences","con_occurrences","first_seen_email_id","last_seen_email_id","updated_at"],
        facts_master_rows
    )
    write_csv(
        os.path.join(args.outdir, "arguments_master.csv"),
        ["arg_line_id","arg_instance_id","arg_ref_id","arg_ref_name","argument_text","email_id","created_at",
         "speaker_name","speaker_email","speaker_role","topic_id","topic_side","reasoning_name","sophism_name","sophism_category",
         "fact_ref_id","fact_veracity_pct","relevance","reasoning_credibility","impact_score","impact_direction",
         "hidden_topic_hint","hidden_topic_confidence","strength","needs_review","notes"],
        arg_lines
    )
    write_csv(
        os.path.join(args.outdir, "topics_master.csv"),
        ["topic_id","topic_name","visibility","hidden_notes","owner_speaker","created_at","updated_at"],
        topics_master_rows
    )
    write_csv(
        os.path.join(args.outdir, "topics_argrefs_table.csv"),
        ["topic_id","topic_name","arg_ref_id","arg_ref_name","side",
         "count_occurrences","sum_strength","avg_strength","max_strength","tuned_strength"],
        topics_argrefs_table_rows
    )
    write_csv(
        os.path.join(args.outdir, "topics_factrefs_table.csv"),
        ["topic_id","topic_name","fact_ref_id","side",
         "count_occurrences","sum_strength","avg_strength","max_strength","tuned_strength"],
        topics_factrefs_table_rows
    )
    write_csv(
        os.path.join(args.outdir, "topics_rollup.csv"),
        ["topic_id","topic_name","visibility",
         "pro_total_strength","con_total_strength",
         "pro_total_strength_tuned","con_total_strength_tuned",
         "pro_unique_args","con_unique_args",
         "open_ratio_hidden_hints","top_hidden_note"],
        topics_rollup_rows
    )
    write_csv(
        os.path.join(args.outdir, "topics_conclusions.csv"),
        ["topic_id","topic_name","visibility",
         "pro_total_tuned","con_total_tuned","net_score",
         "pro_argrefs","con_argrefs","note_hidden"],
        topics_conclusions_rows
    )

    # Simple HTML with a 2-column table per topic (PRO left, CON right)
    html_path = os.path.join(args.outdir, "topics_conclusions.html")
    with open(html_path, "w", encoding="utf-8") as hf:
        hf.write("<!doctype html><meta charset='utf-8'>\n<style>body{font-family:sans-serif} .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin:16px 0} .topic{border:1px solid #ddd;padding:12px;border-radius:8px;margin:12px 0} .pro{background:#f1fff1} .con{background:#fff1f1} .pill{display:inline-block;padding:2px 8px;border-radius:12px;background:#eee;margin-left:8px} .score{font-weight:700} li{margin:4px 0}</style>\n<h1>Topics — PRO vs CON</h1>\n")
        for row in topics_conclusions_rows:
            hf.write(f"<div id='topic-{row['topic_id']}' class='topic'>\n")
            hf.write(f"<h2>{row['topic_name']} <span class='pill'>visibilité: {row['visibility']}</span> <span class='pill score'>net: {row['net_score']:+}</span></h2>\n")
            if row.get("note_hidden"):
                hf.write(f"<div><i>Hypothèse de sujet caché:</i> {row['note_hidden']}</div>")
            # lists
            def parse_list(s):
                # "arg_ref_id::name::strength || ..."  → list of tuples
                out = []
                if not s.strip():
                    return out
                for part in s.split("||"):
                    part = part.strip()
                    if not part:
                        continue
                    bits = [b.strip() for b in part.split("::")]
                    if len(bits) == 3:
                        out.append((bits[0], bits[1], bits[2]))
                return out
            pros = parse_list(row["pro_argrefs"])
            cons = parse_list(row["con_argrefs"])
            hf.write("<div class='grid'>\n<div class='pro'><h3>PRO</h3><ul>\n")
            for ar, nm, st in pros:
                hf.write(f"<li><b>{nm}</b> <span class='pill'>{st}</span> <small>({ar})</small></li>\n")
            hf.write("</ul></div>\n<div class='con'><h3>CON</h3><ul>\n")
            for ar, nm, st in cons:
                hf.write(f"<li><b>{nm}</b> <span class='pill'>{st}</span> <small>({ar})</small></li>\n")
            hf.write("</ul></div>\n</div>\n</div>\n")

    print(f"""✅ OK. Fichiers écrits dans: {args.outdir}
   - facts_master.csv
   - arguments_master.csv
   - topics_master.csv
   - topics_argrefs_table.csv   (tuned_strength incl.)
   - topics_factrefs_table.csv  (tuned_strength incl.)
   - topics_rollup.csv          (base & tuned totals)
   - topics_conclusions.csv     (PRO/CON lists + net_score)
   - topics_conclusions.html    (nice 2-column layout)
""".rstrip())




if __name__ == "__main__":
    main()
