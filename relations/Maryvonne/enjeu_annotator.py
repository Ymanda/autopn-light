# annotate_enjeux.py
# But : lire emails_YYYY.txt, proposer des ENJEUX (dico + IA), produire :
# - /analysis/enjeux_json/YYYY/msg_{n}.json (1 JSON / message)
# - /analysis/enjeux_marked/emails_YYYY_enjeu.txt (texte d'origine + balises ENJEU)
# - /analysis/enjeux_review/proposals_all_years.csv (vue unique, dédupliquée)
#
# Hypothèses structure :
#   SOURCE_FOLDER = "relations/Maryvonne/txt_y.mandaba_full1"
#   ENJEUX_CSV    = "relations/Maryvonne/enjeux_index.csv"
#   OUTPUT_JSON   = "relations/Maryvonne/analysis/enjeux_json"
#   OUTPUT_MARKED = "relations/Maryvonne/analysis/enjeux_marked"
#   OUTPUT_REVIEW = "relations/Maryvonne/analysis/enjeux_review/proposals_all_years.csv"
#
# CSV attendu :
#   index,etiquette,parent_index,synonymes,category,duration,source,created_at,updated_at,owner_email
# - synonymes: "a|b|c" ; category: MAJ|MIN|NEG ; duration: LT|CT ; source: USR|AI
#
# Dédup & fusion :
# - On consolide toutes les propositions IA (toutes années) dans une vue unique.
# - Pour chaque libellé IA : d’abord exact/synonyme existant (normalisé) ; sinon fuzzy (Jaccard).
# - Seulement si rien ne matche : on ajoute AU CSV (AUTO_APPEND_NEW = True par défaut).
# - On écrit un CSV de revue listant action: mapped_existing / appended_new / skipped_duplicate.

import os
import re
import csv
import json
import unicodedata
from datetime import datetime

# ===================== CONFIG =====================

SOURCE_FOLDER = "relations/Maryvonne/txt_y.mandaba_full1"
ENJEUX_CSV = "relations/Maryvonne/enjeux_index.csv"

OUTPUT_JSON_DIR = "relations/Maryvonne/analysis/enjeux_json"
OUTPUT_MARKED_DIR = "relations/Maryvonne/analysis/enjeux_marked"
OUTPUT_REVIEW_DIR = "relations/Maryvonne/analysis/enjeux_review"
OUTPUT_REVIEW_CSV = os.path.join(OUTPUT_REVIEW_DIR, "proposals_all_years.csv")

CONTEXT_SPAN = 5
AUTO_APPEND_NEW = True        # << activé selon ta demande
DEFAULT_NEW_CATEGORY = "MIN"
DEFAULT_NEW_DURATION = "LT"
DEFAULT_NEW_SOURCE = "AI"
DEFAULT_OWNER = "AI"

# ================== PARSING EMAILS =================

EMAIL_BLOCK = re.compile(r"=== MESSAGE ===(.*?)=== FIN ===", re.S)
FIELD_DATE = re.compile(r"^\s*🗕\s*Date\s*:\s*(.+)$", re.M)
FIELD_FROM = re.compile(r"^\s*👤\s*From\s*:\s*(.+)$", re.M)
FIELD_TO   = re.compile(r"^\s*📨\s*To\s*:\s*(.+)$", re.M)
FIELD_SUBJ = re.compile(r"^\s*🧕\s*Subject\s*:\s*(.+)$", re.M)
SEP_LINE   = re.compile(r"^\s*---\s*$", re.M)

def find_email_files(src_folder):
    files = []
    if not os.path.exists(src_folder):
        return files
    for fn in os.listdir(src_folder):
        if fn.startswith("emails_") and fn.endswith(".txt"):
            files.append(os.path.join(src_folder, fn))
    def year_key(path):
        base = os.path.basename(path)
        m = re.search(r"emails_(\d{4})\.txt", base)
        return int(m.group(1)) if m else 0
    return sorted(files, key=year_key)

def split_messages(content):
    return [blk.strip() for blk in EMAIL_BLOCK.findall(content)]

def parse_one_message(block_text):
    date = FIELD_DATE.search(block_text)
    from_ = FIELD_FROM.search(block_text)
    to = FIELD_TO.search(block_text)
    subject = FIELD_SUBJ.search(block_text)
    body = ""
    sep = SEP_LINE.search(block_text)
    if sep:
        body = block_text[sep.end():].strip()
    return {
        "date": (date.group(1).strip() if date else ""),
        "from": (from_.group(1).strip() if from_ else ""),
        "to": (to.group(1).strip() if to else ""),
        "subject": (subject.group(1).strip() if subject else ""),
        "body": body,
        "raw": block_text
    }

def _S(row, *keys) -> str:
    for k in keys:
        if k in row:
            v = row.get(k)
            return "" if v is None else str(v).strip()
    return ""

def _deaccent_lower(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower().strip()

def _normalize_label(s: str) -> str:
    """Normalisation agressive pour dédup : deaccent, lower, remplace / - _ par espace, supprime ponctuation, squeeze espaces."""
    if not s:
        return ""
    s = _deaccent_lower(s)
    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.U)  # enlève ponctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _token_set(s: str) -> set:
    return set(_normalize_label(s).split())

def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

# ================== ENJEUX CSV =====================

def load_enjeux_csv(path):
    """Charge enjeux_index.csv → dict index->record et liste pour matching (tokens)."""
    by_index = {}
    search_entries = []
    max_index = 0
    if not os.path.exists(path):
        return by_index, search_entries, max_index

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        except Exception:
            dialect = csv.excel

        raw_reader = csv.DictReader(f, dialect=dialect)

        for raw_row in raw_reader:
            if not raw_row:
                continue
            if all((v is None or str(v).strip() == "") for v in raw_row.values()):
                continue

            row = {}
            for k, v in raw_row.items():
                key = _deaccent_lower((k or "").lstrip("\ufeff"))
                row[key] = v

            # Tolère "ndex" et "owner_emailindex" (en-tête abîmée)
            idx_str       = _S(row, "index", "id", "ndex")
            etiquette     = _S(row, "etiquette", "label")
            parent_str    = _S(row, "parent_index", "parent")
            synonyms      = _S(row, "synonymes", "synonyms")
            category      = _S(row, "category") or "MIN"
            duration      = _S(row, "duration") or "LT"
            source        = _S(row, "source") or "AI"
            created_at    = _S(row, "created_at")
            updated_at    = _S(row, "updated_at")
            owner         = _S(row, "owner_email", "owner", "owner_emailindex")

            try:
                idx = int(idx_str)
            except Exception:
                continue

            parent_index = None
            try:
                if parent_str:
                    parent_index = int(parent_str)
            except Exception:
                parent_index = None

            synonyms_list = [s.strip() for s in (synonyms.split("|") if synonyms else []) if s.strip()]

            by_index[idx] = {
                "index": idx,
                "etiquette": etiquette,
                "parent_index": parent_index,
                "synonymes": synonyms_list,
                "category": category,
                "duration": duration,
                "source": source,
                "created_at": created_at,
                "updated_at": updated_at,
                "owner_email": owner
            }
            max_index = max(max_index, idx)

            tokens = []
            if etiquette:
                tokens.append(etiquette.lower())
            tokens += [s.lower() for s in synonyms_list if s]
            tokens = [t for t in tokens if t]

            search_entries.append({
                "index": idx,
                "etiquette": etiquette,
                "category": category,
                "duration": duration,
                "source": source,
                "tokens": tokens
            })

    search_entries.sort(key=lambda e: max((len(t) for t in e["tokens"]), default=0), reverse=True)
    return by_index, search_entries, max_index

def append_enjeu_csv(path, new_idx, etiquette, parent_idx, category, duration, source, owner):
    exists = os.path.exists(path)
    fieldnames = ["index","etiquette","parent_index","synonymes","category","duration","source","created_at","updated_at","owner_email"]
    now = datetime.utcnow().strftime("%Y-%m-%d")
    row = {
        "index": new_idx,
        "etiquette": etiquette,
        "parent_index": parent_idx if parent_idx is not None else "",
        "synonymes": "",
        "category": category,
        "duration": duration,
        "source": source,
        "created_at": now,
        "updated_at": now,
        "owner_email": owner
    }
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)

# ================== DETECTION HEURISTIQUE =====================

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").lower().strip()

def detect_from_dictionary(msg, ctx_before, ctx_after, search_entries):
    body_n = normalize_text(msg["body"])
    subject_n = normalize_text(msg["subject"])
    ctx_n = normalize_text(" ".join([c["body"] for c in (ctx_before + ctx_after)]))

    found = []
    for ent in search_entries:
        if not ent["tokens"]:
            continue
        score = 0
        hit_places = []
        for tok in ent["tokens"]:
            if not tok:
                continue
            if tok in body_n:
                score += 2; hit_places.append("body")
            if tok in subject_n:
                score += 1; hit_places.append("subject")
            if tok in ctx_n:
                score += 1; hit_places.append("context")
        if score > 0:
            found.append({
                "index": ent["index"],
                "etiquette": ent["etiquette"],
                "category": ent["category"],
                "duration": ent["duration"],
                "source": ent["source"],
                "score": score,
                "matched_from": sorted(set(hit_places))
            })
    best_by_idx = {}
    for it in found:
        cur = best_by_idx.get(it["index"])
        if not cur or it["score"] > cur["score"]:
            best_by_idx[it["index"]] = it
    return sorted(best_by_idx.values(), key=lambda x: (-x["score"], x["index"]))

def detect_enjeux_ai(msg, ctx_before, ctx_after):
    body = normalize_text(msg["body"])
    candidates = []
    heuristics = [
        ("audit", "Audit comptable / bourse"),
        ("bourse", "Dossier de bourse Tristan"),
        ("dhl", "Paiement DHL / expédition"),
        ("wu", "Paiements via Western Union"),
        ("salaire", "Paiement salaires atelier"),
        ("visa", "Visa / immigration"),
        ("impot", "Impôts / fiscalité"),
        ("pergolese", "Appartement Pergolèse (vente/location)"),
        ("pergolèse", "Appartement Pergolèse (vente/location)"),
        ("toaziki", "Maison Toaziki (usage/location)"),
        ("ecole", "Frais d'école / bus Tristan"),
        ("école", "Frais d'école / bus Tristan"),
        ("bus", "Frais bus Tristan"),
        ("metal", "Achat métal (argent) pour prod"),
        ("métal", "Achat métal (argent) pour prod"),
    ]
    for key, label in heuristics:
        if key in body:
            candidates.append({"label": label, "rationale": f"Mot-clé détecté: {key}", "score": 0.5})
    return candidates

# ================== MARQUAGE TEXTE =====================

def insert_tags_whole_message(raw_block, proposals):
    if not proposals:
        return raw_block
    header_lines = [f"[[[#{p['index']} {p['etiquette']} ({p['category']},{p['duration']})]]]" for p in proposals]
    header = "\n".join(header_lines)
    return f"{header}\n{raw_block}\n[[[END]]]"

# ================== JSON PAR MESSAGE =====================

def write_msg_json(out_dir, year, seq, payload):
    ydir = os.path.join(out_dir, str(year))
    os.makedirs(ydir, exist_ok=True)
    path = os.path.join(ydir, f"msg_{seq:05d}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ================== CONSOLIDATION MULTI-ANNEES =====================

def build_existing_index(enjeux_dict):
    """
    Construit les index normalisés → {norm_label: idx}
    et une liste (idx, etiquette, tokenset) pour fuzzy match.
    """
    exact_map = {}   # norm_label -> idx
    fuzzy_list = []  # tuples (idx, etiquette, token_set)

    for idx, rec in enjeux_dict.items():
        lab = rec.get("etiquette", "") or ""
        norm = _normalize_label(lab)
        if norm:
            exact_map[norm] = idx
            fuzzy_list.append((idx, lab, _token_set(lab)))
        for syn in rec.get("synonymes", []) or []:
            ns = _normalize_label(syn)
            if ns:
                exact_map[ns] = idx
                fuzzy_list.append((idx, syn, _token_set(syn)))
    return exact_map, fuzzy_list

def consolidate_and_update(enjeux_dict, proposals_global):
    """
    proposals_global: dict norm_label -> {
        'labels': set[str], 'occurrences': [(year, seq, subject_excerpt)]
    }
    Retourne un rapport et, si AUTO_APPEND_NEW, ajoute au CSV.
    """
    exact_map, fuzzy_list = build_existing_index(enjeux_dict)
    current_max = max((k for k in enjeux_dict.keys()), default=0)

    rows_review = []  # pour CSV de revue

    for norm_label, info in sorted(proposals_global.items(), key=lambda kv: kv[0]):
        original_variants = sorted(info["labels"])
        first_label = original_variants[0] if original_variants else norm_label

        # 1) Exact / synonyme existant
        mapped_idx = exact_map.get(norm_label)
        action = ""
        canonical = ""

        if mapped_idx:
            action = "mapped_existing"
            canonical = enjeux_dict[mapped_idx]["etiquette"] or first_label
        else:
            # 2) Fuzzy Jaccard
            ts = _token_set(first_label)
            best = (0.0, None, None)  # (score, idx, etiquette)
            for idx, lab, ts_lab in fuzzy_list:
                score = _jaccard(ts, ts_lab)
                if score > best[0]:
                    best = (score, idx, lab)
            if best[0] >= 0.75:  # seuil ajustable
                mapped_idx = best[1]
                action = f"mapped_fuzzy_{best[0]:.2f}"
                canonical = enjeux_dict[mapped_idx]["etiquette"] or best[2]

        if not mapped_idx:
            # 3) Nouveau : append si activé
            if AUTO_APPEND_NEW:
                current_max += 1
                new_idx = current_max
                append_enjeu_csv(
                    ENJEUX_CSV, new_idx, first_label, new_idx,
                    DEFAULT_NEW_CATEGORY, DEFAULT_NEW_DURATION,
                    DEFAULT_NEW_SOURCE, DEFAULT_OWNER
                )
                enjeux_dict[new_idx] = {
                    "index": new_idx,
                    "etiquette": first_label,
                    "parent_index": new_idx,
                    "synonymes": [],
                    "category": DEFAULT_NEW_CATEGORY,
                    "duration": DEFAULT_NEW_DURATION,
                    "source": DEFAULT_NEW_SOURCE,
                    "created_at": datetime.utcnow().strftime("%Y-%m-%d"),
                    "updated_at": datetime.utcnow().strftime("%Y-%m-%d"),
                    "owner_email": DEFAULT_OWNER
                }
                action = "appended_new"
                mapped_idx = new_idx
                canonical = first_label
            else:
                action = "new_candidate"
                canonical = first_label

        # Prépare la ligne de revue
        years = sorted(set(y for (y, _, _) in info["occurrences"]))
        sample = "; ".join(sorted(original_variants))[:200]
        rows_review.append({
            "norm_label": norm_label,
            "canonical_label": canonical,
            "mapped_index": mapped_idx,
            "action": action,
            "years": "|".join(map(str, years)),
            "examples": sample
        })

    # Écrit le CSV de revue unique
    os.makedirs(OUTPUT_REVIEW_DIR, exist_ok=True)
    with open(OUTPUT_REVIEW_CSV, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["norm_label","canonical_label","mapped_index","action","years","examples"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows_review:
            w.writerow(r)

    return rows_review

# ================== MAIN PIPELINE =====================

def process_year_file(path, enjeux_dict, search_entries, max_index, proposals_global):
    base = os.path.basename(path)
    m = re.search(r"emails_(\d{4})\.txt", base)
    year = int(m.group(1)) if m else 0

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    blocks = split_messages(content)
    messages = [parse_one_message(b) for b in blocks]

    os.makedirs(OUTPUT_MARKED_DIR, exist_ok=True)
    marked_out_path = os.path.join(OUTPUT_MARKED_DIR, f"emails_{year}_enjeu.txt")
    with open(marked_out_path, "w", encoding="utf-8") as mout:
        mout.write("")

    for i, msg in enumerate(messages, start=1):
        # contexte
        start_b = max(0, i-1-CONTEXT_SPAN)
        end_a = min(len(messages), i+CONTEXT_SPAN)
        ctx_before = messages[start_b: i-1]
        ctx_after  = messages[i: end_a]

        # 1) Dictionnaire
        props_dict = detect_from_dictionary(msg, ctx_before, ctx_after, search_entries)

        # 2) IA (stub)
        props_ai = []
        for c in detect_enjeux_ai(msg, ctx_before, ctx_after):
            label = c["label"].strip()
            if not label:
                continue
            if any(label.lower() == (p["etiquette"] or "").lower() for p in props_dict):
                continue
            existing = [e for e in search_entries if (e["etiquette"] or "").lower() == label.lower()]
            if existing:
                ent = existing[0]
                props_ai.append({
                    "index": ent["index"],
                    "etiquette": ent["etiquette"],
                    "category": ent["category"],
                    "duration": ent["duration"],
                    "source": ent["source"],
                    "score": 1,
                    "matched_from": ["ai"]
                })
            else:
                # AJOUT AU POOL GLOBAL (dédup multi-années)
                norm = _normalize_label(label)
                occ = (year, i, (msg["subject"] or "")[:120])
                slot = proposals_global.setdefault(norm, {"labels": set(), "occurrences": []})
                slot["labels"].add(label)
                slot["occurrences"].append(occ)

                # On garde quand même une balise provisoire (index négatif local)
                props_ai.append({
                    "index": -i,  # local
                    "etiquette": label,
                    "category": DEFAULT_NEW_CATEGORY,
                    "duration": DEFAULT_NEW_DURATION,
                    "source": DEFAULT_NEW_SOURCE,
                    "score": 1,
                    "matched_from": ["ai_new"]
                })

        # Fusion
        proposals = props_dict + props_ai

        # JSON par message
        payload = {
            "year": year,
            "seq": i,
            "meta": {
                "date": msg["date"],
                "from": msg["from"],
                "to": msg["to"],
                "subject": msg["subject"]
            },
            "proposals": proposals,
            "raw_excerpt": msg["raw"][:4000]
        }
        write_msg_json(OUTPUT_JSON_DIR, year, i, payload)

        # Marquage texte (message entier)
        marked = insert_tags_whole_message(msg["raw"], proposals)
        with open(marked_out_path, "a", encoding="utf-8") as mout:
            mout.write(marked + "\n\n")

    # Affichage de synthèse annuelle (optionnel)
    # On ne liste plus ici les nouveaux labels IA; la consolidation globale s'en charge.

def main():
    # Dico d’enjeux existant
    enjeux_dict, search_entries, max_index = load_enjeux_csv(ENJEUX_CSV)
    if not search_entries:
        print(f"⚠️ Dictionnaire vide ou introuvable: {ENJEUX_CSV}")

    files = find_email_files(SOURCE_FOLDER)
    if not files:
        print(f"❌ Aucun emails_YYYY.txt trouvé dans {SOURCE_FOLDER}")
        return

    os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)
    os.makedirs(OUTPUT_MARKED_DIR, exist_ok=True)
    os.makedirs(OUTPUT_REVIEW_DIR, exist_ok=True)

    # Pool global des propositions IA (toutes années)
    proposals_global = {}

    for fpath in files:
        print(f"\n▶️ Traitement : {fpath}")
        process_year_file(fpath, enjeux_dict, search_entries, max_index, proposals_global)

    # Consolidation & mise à jour éventuelle du CSV
    print("\n🔄 Consolidation multi-années & fusion avec le dictionnaire…")
    rows = consolidate_and_update(enjeux_dict, proposals_global)

    print(f"✅ Annotation terminée.")
    print(f"🗂 Revue unique (toutes années) : {OUTPUT_REVIEW_CSV}")
    # Petit résumé
    mapped = sum(1 for r in rows if r["action"].startswith("mapped"))
    appended = sum(1 for r in rows if r["action"] == "appended_new")
    newcand = sum(1 for r in rows if r["action"] == "new_candidate")
    print(f"   • Mappés vers existants : {mapped}")
    print(f"   • Ajouts au CSV         : {appended}  (AUTO_APPEND_NEW={AUTO_APPEND_NEW})")
    print(f"   • Restent en candidats  : {newcand}")

if __name__ == "__main__":
    main()
