# annotate_enjeux.py
# But : lire emails_YYYY.txt, proposer des ENJEUX (dico + IA), produire :
# - /analysis/enjeux_json/YYYY/msg_{n}.json (1 JSON / message)
# - /analysis/enjeux_marked/emails_YYYY_enjeu.txt (texte d'origine + balises ENJEU)
#
# Hypothèses structure :
#   SOURCE_FOLDER = "relations/Maryvonne/txt_y.mandaba"
#   ENJEUX_CSV    = "relations/Maryvonne/enjeux_index.csv"
#   OUTPUT_JSON   = "relations/Maryvonne/analysis/enjeux_json"
#   OUTPUT_MARKED = "relations/Maryvonne/analysis/enjeux_marked"
#
# CSV attendu (en-tête incluse) :
#   index,etiquette,parent_index,synonymes,category,duration,source,created_at,updated_at,owner_email
# - synonymes: séparés par "|" (ex: "frais d'école|bus scolaire")
# - category: MAJ|MIN|NEG
# - duration: LT|CT
# - source: USR|AI
#
# Balises insérées dans *_enjeu.txt :
#   [[[#{index} {etiquette} ({category},{duration})]]]
#   ... (segment du message correspondant ou message entier si global) ...
#   [[[END]]]
#
# Notes :
# - Le repérage des segments est simplifié (on balise le message entier quand ambigu).
# - Contexte : 2 messages avant et 2 après (configurable).
# - IA : fonction stub detect_enjeux_ai(); à remplacer par appel API plus tard.
# - Par défaut, pas d'ajout auto au CSV (AUTO_APPEND_NEW = False).
# - Détection robustifiée : normalisation, recherche etiquette + synonymes (contains).
# - Si plusieurs enjeux matchent, on marque plusieurs balises (priorité par longueur d’étiquette).
# - Les JSON par message contiennent toutes les propositions avec score simple (heuristique).

import os
import re
import csv
import json
from datetime import datetime
from collections import defaultdict

# ===================== CONFIG =====================

SOURCE_FOLDER = "relations/Maryvonne/txt_y.mandaba"
ENJEUX_CSV = "relations/Maryvonne/enjeux_index.csv"

OUTPUT_JSON_DIR = "relations/Maryvonne/analysis/enjeux_json"
OUTPUT_MARKED_DIR = "relations/Maryvonne/analysis/enjeux_marked"

CONTEXT_SPAN = 2           # nb de messages avant/après pour contexte étendu
AUTO_APPEND_NEW = False    # si True, ajoute au CSV les nouveaux enjeux IA
DEFAULT_NEW_CATEGORY = "MIN"
DEFAULT_NEW_DURATION = "LT"
DEFAULT_NEW_SOURCE = "AI"
DEFAULT_OWNER = "AI"

# ================== PARSING EMAILS =================

EMAIL_BLOCK = re.compile(r"=== MESSAGE ===(.*?)=== FIN ===", re.S)
FIELD_DATE = re.compile(r"^\s*🗕\s*Date\s*:\s*(.+)$", re.M)
FIELD_FROM = re.compile(r"^\s*👤\s*From\s*:\s*(.+)$", re.M)
FIELD_TO   = re.compile(r"^\s*📨\s*To\s*:\s*(.+)$",   re.M)
FIELD_SUBJ = re.compile(r"^\s*🧕\s*Subject\s*:\s*(.+)$", re.M)
SEP_LINE   = re.compile(r"^\s*---\s*$", re.M)

def find_email_files(src_folder):
    files = []
    if not os.path.exists(src_folder):
        return files
    for fn in os.listdir(src_folder):
        if fn.startswith("emails_") and fn.endswith(".txt"):
            files.append(os.path.join(src_folder, fn))
    # Tri par année si possible
    def year_key(path):
        base = os.path.basename(path)
        m = re.search(r"emails_(\d{4})\.txt", base)
        return int(m.group(1)) if m else 0
    return sorted(files, key=year_key)

def split_messages(content):
    # Retourne liste de blocs texte (sans les délimiteurs)
    return [blk.strip() for blk in EMAIL_BLOCK.findall(content)]

def parse_one_message(block_text):
    date = FIELD_DATE.search(block_text)
    from_ = FIELD_FROM.search(block_text)
    to = FIELD_TO.search(block_text)
    subject = FIELD_SUBJ.search(block_text)
    # corps = après la ligne '---'
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

# ================== ENJEUX CSV =====================

def load_enjeux_csv(path):
    """Charge enjeux_index.csv → dict index->record et listes pour matching."""
    by_index = {}
    search_entries = []  # liste de (index, etiquette, category, duration, source, tokens[])
    max_index = 0
    if not os.path.exists(path):
        return by_index, search_entries, max_index

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                idx = int(row["index"])
            except:
                continue
            etiquette = row.get("etiquette", "").strip()
            parent_index = row.get("parent_index", "").strip()
            parent_index = int(parent_index) if parent_index.isdigit() else None
            synonyms = row.get("synonymes", "").strip()
            synonyms_list = [s.strip() for s in synonyms.split("|") if s.strip()]
            category = row.get("category", "MIN").strip() or "MIN"
            duration = row.get("duration", "LT").strip() or "LT"
            source = row.get("source", "AI").strip() or "AI"
            created_at = row.get("created_at", "").strip()
            updated_at = row.get("updated_at", "").strip()
            owner = row.get("owner_email", "").strip()

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

            tokens = [etiquette.lower()] if etiquette else []
            tokens += [s.lower() for s in synonyms_list]
            tokens = [t for t in tokens if t]
            search_entries.append({
                "index": idx,
                "etiquette": etiquette,
                "category": category,
                "duration": duration,
                "source": source,
                "tokens": tokens
            })
    # Priorité : termes plus longs d'abord
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
    """
    Retourne liste de propositions depuis le dictionnaire :
    [{index, etiquette, category, duration, source, score, matched_from: ['body','subject',...]}]
    Score très simpliste : +2 si dans body, +1 si dans subject, +1 si dans contexte.
    """
    body_n = normalize_text(msg["body"])
    subject_n = normalize_text(msg["subject"])
    ctx_n = normalize_text(" ".join([c["body"] for c in (ctx_before + ctx_after)]))

    found = []
    for ent in search_entries:
        score = 0
        hit_places = []
        for tok in ent["tokens"]:
            if not tok:
                continue
            if tok in body_n:
                score += 2
                hit_places.append("body")
            if tok in subject_n:
                score += 1
                hit_places.append("subject")
            if tok in ctx_n:
                score += 1
                hit_places.append("context")
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
    # dédupe par index en gardant le meilleur score
    best_by_idx = {}
    for it in found:
        cur = best_by_idx.get(it["index"])
        if not cur or it["score"] > cur["score"]:
            best_by_idx[it["index"]] = it
    return sorted(best_by_idx.values(), key=lambda x: (-x["score"], x["index"]))

def detect_enjeux_ai(msg, ctx_before, ctx_after):
    """
    Stub IA : retourne liste de propositions de nouveaux enjeux (phrase candidate).
    Format :
      [{"label": "Financement audit bourse", "rationale": "...", "score": 0.55}]
    >>> À remplacer plus tard par un appel API GPT avec consignes et base_context.
    """
    # Heuristique légère pour l'exemple : repérer termes génériques pour proposer labels bruts
    body = normalize_text(msg["body"])
    candidates = []
    heuristics = [
        ("audit", "Audit comptable / bourse"),
        ("bourse", "Dossier de bourse Tristan"),
        ("dhl", "Paiement DHL / expédition"),
        ("wu", "Paiements via Western Union"),
        ("salaire", "Paiement salaires atelier"),
        ("visa", "Visa / immigration"),
        ("impôt", "Impôts / fiscalité"),
        ("pergolèse", "Appartement Pergolèse (vente/location)"),
        ("toaziki", "Maison Toaziki (usage/location)"),
        ("école", "Frais d'école / bus Tristan"),
        ("bus", "Frais bus Tristan"),
        ("métal", "Achat métal (argent) pour prod")
    ]
    for key, label in heuristics:
        if key in body:
            candidates.append({"label": label, "rationale": f"Mot-clé détecté: {key}", "score": 0.5})
    return candidates

# ================== MARQUAGE TEXTE =====================

def insert_tags_whole_message(raw_block, proposals):
    """
    Pour l’instant : on encadre l’ENTIER du message par une balise par enjeu (si plusieurs, on concatène les en-têtes).
    Variante simple, robuste, sans tenter d’aligner des sous-spans.
    """
    if not proposals:
        return raw_block

    header_lines = []
    for p in proposals:
        header_lines.append(f"[[[#{p['index']} {p['etiquette']} ({p['category']},{p['duration']})]]]")
    header = "\n".join(header_lines)
    return f"{header}\n{raw_block}\n[[[END]]]"

# ================== JSON PAR MESSAGE =====================

def write_msg_json(out_dir, year, seq, payload):
    ydir = os.path.join(out_dir, str(year))
    os.makedirs(ydir, exist_ok=True)
    path = os.path.join(ydir, f"msg_{seq:05d}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ================== MAIN PIPELINE =====================

def process_year_file(path, enjeux_dict, search_entries, max_index):
    base = os.path.basename(path)
    m = re.search(r"emails_(\d{4})\.txt", base)
    year = int(m.group(1)) if m else 0

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    blocks = split_messages(content)
    messages = [parse_one_message(b) for b in blocks]

    # Marqué out
    os.makedirs(OUTPUT_MARKED_DIR, exist_ok=True)
    marked_out_path = os.path.join(OUTPUT_MARKED_DIR, f"emails_{year}_enjeu.txt")
    with open(marked_out_path, "w", encoding="utf-8") as mout:
        mout.write("")  # reset

    new_labels_to_add = []  # pour rapport fin d'année
    next_index = max_index

    for i, msg in enumerate(messages, start=1):
        ctx_before = messages[max(0, i-1-CONTEXT_SPAN): i-1]
        ctx_after = messages[i: min(len(messages), i-1+1+CONTEXT_SPAN)]

        # 1) Enjeux connus (dico)
        props_dict = detect_from_dictionary(msg, ctx_before, ctx_after, search_entries)

        # 2) Enjeux IA (stub) → propositions à confirmer
        props_ai = []
        for c in detect_enjeux_ai(msg, ctx_before, ctx_after):
            label = c["label"].strip()
            if not label:
                continue
            # Eviter doublon si déjà matché par dico
            if any(label.lower() == p["etiquette"].lower() for p in props_dict):
                continue
            # Rechercher si déjà dans CSV (étiquette exacte)
            existing = [e for e in search_entries if e["etiquette"].lower() == label.lower()]
            if existing:
                ent = existing[0]
                props_ai.append({
                    "index": ent["index"],
                    "etiquette": ent["etiquette"],
                    "category": ent["category"],
                    "duration": ent["duration"],
                    "source": ent["source"],
                    "score": max(1, int(c["score"] * 2)),  # petit score
                    "matched_from": ["ai"]
                })
            else:
                # Nouveau candidat → index provisoire négatif (local) pour JSON/marquage
                next_index -= 1
                props_ai.append({
                    "index": next_index,  # index provisoire négatif
                    "etiquette": label,
                    "category": DEFAULT_NEW_CATEGORY,
                    "duration": DEFAULT_NEW_DURATION,
                    "source": DEFAULT_NEW_SOURCE,
                    "score": max(1, int(c["score"] * 2)),
                    "matched_from": ["ai_new"]
                })
                new_labels_to_add.append(label)

        # Fusion des propositions (dico d’abord, puis IA)
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
            "raw_excerpt": msg["raw"][:4000]  # pour inspection rapide
        }
        write_msg_json(OUTPUT_JSON_DIR, year, i, payload)

        # Marquage texte (simple : message entier)
        marked = insert_tags_whole_message(msg["raw"], proposals)
        with open(os.path.join(OUTPUT_MARKED_DIR, f"emails_{year}_enjeu.txt"), "a", encoding="utf-8") as mout:
            mout.write(marked + "\n\n")

    # Rapport fin d’année : nouveaux labels IA proposés
    new_labels_unique = sorted(set(new_labels_to_add), key=lambda s: s.lower())
    if new_labels_unique:
        print(f"📌 {year}: nouveaux enjeux IA proposés ({len(new_labels_unique)}) :")
        for lab in new_labels_unique:
            print(f"   - {lab}")
        if AUTO_APPEND_NEW:
            # Append au CSV avec nouveaux index positifs
            print("✍️ Ajout auto dans enjeux_index.csv activé.")
            current_max = max((k for k in enjeux_dict.keys()), default=0)
            for lab in new_labels_unique:
                current_max += 1
                append_enjeu_csv(
                    ENJEUX_CSV, current_max, lab, current_max,  # parent = soi-même
                    DEFAULT_NEW_CATEGORY, DEFAULT_NEW_DURATION, DEFAULT_NEW_SOURCE, DEFAULT_OWNER
                )
    return

def main():
    # Charger le dico d’enjeux
    enjeux_dict, search_entries, max_index = load_enjeux_csv(ENJEUX_CSV)
    if not search_entries:
        print(f"⚠️ Dictionnaire vide ou introuvable: {ENJEUX_CSV} (tu peux le créer d’abord).")

    files = find_email_files(SOURCE_FOLDER)
    if not files:
        print(f"❌ Aucun emails_YYYY.txt trouvé dans {SOURCE_FOLDER}")
        return

    os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)
    os.makedirs(OUTPUT_MARKED_DIR, exist_ok=True)

    for fpath in files:
        print(f"\n▶️ Traitement : {fpath}")
        process_year_file(fpath, enjeux_dict, search_entries, max_index)

    print("\n✅ Annotation terminée. JSON + fichiers marqués disponibles.")

if __name__ == "__main__":
    main()
