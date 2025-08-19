# topics_miner.py
# Analyse volumique des sujets (noms / entités / syntagmes nominaux) sur toutes les années
# Entrées  : relations/Maryvonne/txt_y.mandaba_full1/emails_YYYY.txt
# Dico     : relations/Maryvonne/enjeux_index.csv
# Sorties  : relations/Maryvonne/analysis/topics/
#   - topics_all_years.csv         (mix tokens/entités/phrases nominales)
#   - top_entities.csv             (PERSON/ORG/GPE/LOC)
#   - top_noun_phrases.csv         (phrases nominales)
#   - topics_by_year_YYYY.csv      (top tokens par année)
#
# Dépendances optionnelles : spaCy FR
#   pip install spacy
#   python -m spacy download fr_core_news_md   # ou fr_core_news_sm

import os
import re
import csv
import json
import difflib
import unicodedata
from collections import defaultdict, Counter
from datetime import datetime

# -------------------- CONFIG --------------------

SOURCE_FOLDER = "relations/Maryvonne/txt_y.mandaba_full1"
ENJEUX_CSV    = "relations/Maryvonne/enjeux_index.csv"

OUT_DIR          = "relations/Maryvonne/analysis/topics"
OUT_ALL          = os.path.join(OUT_DIR, "topics_all_years.csv")
OUT_ENT          = os.path.join(OUT_DIR, "top_entities.csv")
OUT_CHUNKS       = os.path.join(OUT_DIR, "top_noun_phrases.csv")
OUT_BY_YEAR_TPL  = os.path.join(OUT_DIR, "topics_by_year_{year}.csv")

EMAIL_BLOCK = re.compile(r"=== MESSAGE ===(.*?)=== FIN ===", re.S)
SEP_LINE    = re.compile(r"^\s*---\s*$", re.M)

# -------------------- UTIL TEXT --------------------

def _deaccent_lower(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower().strip()

def _normalize_label(s: str) -> str:
    if not s:
        return ""
    s = _deaccent_lower(s)
    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.U)  # enlève ponctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _token_set(s: str) -> set:
    return set(_normalize_label(s).split())

def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

# -------------------- STOPWORDS (fallback) --------------------

FR_STOP = {
    "a","afin","ah","ai","aie","aient","aies","ainsi","ait","allaient","allo","allons","alors","apres","après",
    "as","assez","au","aucun","aucune","aujourd","aujourd'hui","aupres","auprès","auquel","aura","aurai","auraient",
    "aurais","aurait","auras","aurez","auriez","aurions","aurons","auront","aussi","autre","autres","aux",
    "auxquelles","auxquels","avaient","avais","avait","avant","avec","avez","aviez","avions","avons","avoir",
    "ayant","ayez","ayons","bon","bonjour","bonne","car","ce","ceci","cela","celle","celles","celui","cependant",
    "certain","certaine","certaines","certains","ces","cet","cette","ceux","chacun","chacune","chaque","chez","ci",
    "combien","comme","comment","compris","d","da","dans","de","debout","dedans","dehors","delà","depuis","des",
    "desormais","désormais","desquelles","desquels","dessous","dessus","deux","devant","devoir","doit","doivent",
    "donc","dont","du","duquel","durant","elle","elles","en","encore","enfin","entre","envers","es","est","et",
    "etaient","étaient","etais","étais","etait","était","etant","étant","etc","ete","été","etre","être","eu","eue",
    "eues","euh","eux","eurent","eus","eusse","eussent","eusses","eussiez","eussions","eut","faire","fais","faisait",
    "faisant","fait","faite","faites","fois","font","hors","ici","il","ils","j","je","jusqu","jusque","l","la",
    "laquelle","le","les","lesquelles","lesquels","leur","leurs","lui","m","ma","maintenant","mais","malgre","malgré",
    "me","meme","mêmes","merci","mes","mien","mienne","miennes","miens","moi","moins","mon","moyennant","n","ne","ni",
    "non","nos","notre","nous","nouveau","nouveaux","nul","nulle","on","ont","ou","où","par","parce","parfois","parmi",
    "pas","pendant","personne","peu","peut","peuvent","peux","plus","plutot","plutôt","pour","pourquoi","prealable",
    "préalable","pres","près","puis","qu","quand","quel","quelle","quelles","quels","qui","quoi","quoique","re","rien",
    "s","sa","sans","se","selon","ses","si","sien","sienne","siennes","siens","soi","soient","sois","soit","sommes",
    "son","sont","soyez","soyons","suis","sur","t","ta","tandis","tel","telle","telles","tels","tes","toi","ton",
    "tous","tout","toute","toutes","tres","très","tu","un","une","va","vers","via","voici","voila","voilà","vos",
    "votre","vous","vu","y","zut","cordialement","merci"
}

# -------------------- PARSING EMAILS --------------------

def find_email_files(src_folder):
    files = []
    if not os.path.exists(src_folder):
        return files
    for fn in os.listdir(src_folder):
        if fn.startswith("emails_") and fn.endswith(".txt"):
            files.append(os.path.join(src_folder, fn))
    def year_key(p):
        m = re.search(r"emails_(\d{4})\.txt", os.path.basename(p))
        return int(m.group(1)) if m else 0
    return sorted(files, key=year_key)

def split_messages(content):
    return [blk.strip() for blk in EMAIL_BLOCK.findall(content)]

def parse_body(block_text):
    # Prend le contenu après '---', ignore les lignes citées '>'
    body = ""
    m = SEP_LINE.search(block_text)
    body = block_text[m.end():] if m else block_text
    lines = []
    for line in body.splitlines():
        if line.strip().startswith(">"):
            continue
        # coupe si "a écrit :" pour ignorer l'historique
        if re.search(r"\b(a écrit|wrote):\s*$", line, flags=re.I):
            break
        lines.append(line)
    txt = "\n".join(lines)
    # Nettoyage de quoted-printable simple
    txt = txt.replace("=\n", "").replace("=20", " ")
    return txt.strip()

# -------------------- DICTIONNAIRE ENJEUX --------------------

def load_enjeux_csv(path):
    by_index = {}
    if not os.path.exists(path):
        return by_index
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        except Exception:
            dialect = csv.excel
        rdr = csv.DictReader(f, dialect=dialect)
        for raw in rdr:
            if not raw:
                continue
            row = {}
            for k, v in raw.items():
                key = _deaccent_lower((k or "").lstrip("\ufeff"))
                row[key] = v
            idx_str = (row.get("index") or row.get("ndex") or "").strip()
            try:
                idx = int(idx_str)
            except:
                continue
            etiquette = (row.get("etiquette") or "").strip()
            syns = (row.get("synonymes") or "").strip()
            syn_list = [s.strip() for s in syns.split("|") if s.strip()]
            by_index[idx] = {"etiquette": etiquette, "synonymes": syn_list}
    return by_index

def build_concept_space(enjeux_dict):
    exact_map = {}   # norm_label -> idx
    concepts = []    # (idx, label, token_set)
    for idx, rec in enjeux_dict.items():
        label = rec.get("etiquette") or ""
        nl = _normalize_label(label)
        if nl:
            exact_map[nl] = idx
            concepts.append((idx, label, _token_set(label)))
        for syn in rec.get("synonymes", []):
            ns = _normalize_label(syn)
            if ns:
                exact_map[ns] = idx
                concepts.append((idx, syn, _token_set(syn)))
    return exact_map, concepts

def map_to_concept(text, exact_map, concepts, jac_threshold=0.70, seq_threshold=0.90):
    """Retourne (idx, method) ou (None, None)."""
    nl = _normalize_label(text)
    if not nl:
        return None, None
    # exact/synonym
    if nl in exact_map:
        return exact_map[nl], "exact_or_synonym"
    # fuzzy jaccard
    ts = _token_set(text)
    best = (0.0, None)
    for idx, lab, ts_lab in concepts:
        sc = jaccard(ts, ts_lab)
        if sc > best[0]:
            best = (sc, idx)
    if best[0] >= jac_threshold:
        return best[1], f"fuzzy_jaccard_{best[0]:.2f}"
    # difflib ratio
    for idx, lab, _ in concepts:
        r = difflib.SequenceMatcher(a=nl, b=_normalize_label(lab)).ratio()
        if r >= seq_threshold:
            return idx, f"fuzzy_seq_{r:.2f}"
    return None, None

# -------------------- NLP (spaCy optionnel) --------------------

def load_spacy():
    try:
        import spacy
        try:
            return spacy.load("fr_core_news_md")
        except Exception:
            return spacy.load("fr_core_news_sm")
    except Exception:
        return None

NLP = load_spacy()
WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿœŒ]+", re.U)

def simple_tokens(text):
    toks = [t for t in WORD_RE.findall(_deaccent_lower(text)) if len(t) >= 3 and t not in FR_STOP]
    return toks

def extract_units(text):
    """
    Retourne:
      nouns: list[str]          (noms / noms propres)
      noun_phrases: list[str]   (phrases nominales)
      entities: list[(label, text)]  PERSON/ORG/GPE/LOC
    """
    if NLP:
        doc = NLP(text)
        nouns = [t.lemma_.lower() for t in doc if t.pos_ in {"NOUN","PROPN"} and t.lemma_]
        nouns = [n for n in nouns if n not in FR_STOP and len(n) >= 3]
        noun_phrases = [re.sub(r"\s+", " ", ch.text.strip()) for ch in doc.noun_chunks if len(ch.text.strip()) >= 3]
        entities = [(ent.label_, ent.text.strip()) for ent in doc.ents if ent.label_ in {"PERSON","ORG","GPE","LOC"}]
        return nouns, noun_phrases, entities
    else:
        toks = simple_tokens(text)
        return toks, [], []  # pas de noun_chunks/entities sans spaCy

# -------------------- PIPELINE --------------------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    enjeux = load_enjeux_csv(ENJEUX_CSV)
    exact_map, concepts = build_concept_space(enjeux)
    print(f"📚 Enjeux chargés: {len(enjeux)} | spaCy: {'OK' if NLP else 'fallback'}")

    files = find_email_files(SOURCE_FOLDER)
    if not files:
        print(f"❌ Aucun emails_YYYY.txt dans {SOURCE_FOLDER}")
        return

    # Compteurs globaux
    token_occ   = Counter()
    token_msgs  = defaultdict(set)
    token_years = defaultdict(set)

    chunk_occ   = Counter()
    chunk_msgs  = defaultdict(set)
    chunk_years = defaultdict(set)

    ent_occ     = Counter()
    ent_msgs    = defaultdict(set)
    ent_years   = defaultdict(set)

    per_year_token_occ  = defaultdict(Counter)
    per_year_token_msgs = defaultdict(lambda: defaultdict(set))

    total_messages = 0

    for path in files:
        m = re.search(r"emails_(\d{4})\.txt", os.path.basename(path))
        year = int(m.group(1)) if m else 0

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        blocks = split_messages(content)

        for i, blk in enumerate(blocks, start=1):
            body = parse_body(blk)
            if not body:
                continue
            total_messages += 1
            msg_id = f"{year}:{i}"

            nouns, noun_phrases, entities = extract_units(body)

            # tokens
            counts = Counter(nouns)
            for tok, c in counts.items():
                token_occ[tok] += c
                token_msgs[tok].add(msg_id)
                token_years[tok].add(year)
                per_year_token_occ[year][tok] += c
                per_year_token_msgs[year][tok].add(msg_id)

            # noun phrases
            if noun_phrases:
                c2 = Counter([_normalize_label(p) for p in noun_phrases if _normalize_label(p)])
                for ch, c in c2.items():
                    if not ch:
                        continue
                    chunk_occ[ch] += c
                    chunk_msgs[ch].add(msg_id)
                    chunk_years[ch].add(year)

            # entities
            for lab, txt in entities:
                key = f"{lab}:{txt}"
                ent_occ[key] += 1
                ent_msgs[key].add(msg_id)
                ent_years[key].add(year)

    # -------------------- Table globale --------------------
    rows_all = []

    # tokens
    for tok in token_occ:
        idx, method = map_to_concept(tok, exact_map, concepts)
        rows_all.append({
            "type": "token",
            "text": tok,
            "messages_count": len(token_msgs[tok]),
            "occurrences": token_occ[tok],
            "years": "|".join(str(y) for y in sorted(token_years[tok])),
            "mapped_index": idx or "",
            "mapped_label": (enjeux[idx]["etiquette"] if idx else ""),
            "match_method": method or ""
        })

    # entities
    for key in ent_occ:
        lab, txt = key.split(":", 1)
        idx, method = map_to_concept(txt, exact_map, concepts)
        rows_all.append({
            "type": f"entity_{lab}",
            "text": txt,
            "messages_count": len(ent_msgs[key]),
            "occurrences": ent_occ[key],
            "years": "|".join(str(y) for y in sorted(ent_years[key])),
            "mapped_index": idx or "",
            "mapped_label": (enjeux[idx]["etiquette"] if idx else ""),
            "match_method": method or ""
        })

    # noun phrases
    for ch in chunk_occ:
        idx, method = map_to_concept(ch, exact_map, concepts)
        rows_all.append({
            "type": "noun_phrase",
            "text": ch,
            "messages_count": len(chunk_msgs[ch]),
            "occurrences": chunk_occ[ch],
            "years": "|".join(str(y) for y in sorted(chunk_years[ch])),
            "mapped_index": idx or "",
            "mapped_label": (enjeux[idx]["etiquette"] if idx else ""),
            "match_method": method or ""
        })

    # Tri principal : nb de messages, puis occurrences, puis ordre alpha
    rows_all.sort(key=lambda r: (-int(r["messages_count"]), -int(r["occurrences"]), r["text"]))

    # Écriture CSV global
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_ALL, "w", encoding="utf-8", newline="") as f:
        fn = ["type","text","messages_count","occurrences","years","mapped_index","mapped_label","match_method"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        for r in rows_all:
            w.writerow(r)

    # Entités seules
    ent_rows = [r for r in rows_all if r["type"].startswith("entity_")]
    with open(OUT_ENT, "w", encoding="utf-8", newline="") as f:
        fn = ["type","text","messages_count","occurrences","years","mapped_index","mapped_label","match_method"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        for r in ent_rows:
            w.writerow(r)

    # Phrases nominales seules
    np_rows = [r for r in rows_all if r["type"] == "noun_phrase"]
    with open(OUT_CHUNKS, "w", encoding="utf-8", newline="") as f:
        fn = ["type","text","messages_count","occurrences","years","mapped_index","mapped_label","match_method"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        for r in np_rows:
            w.writerow(r)

    # Par année (top 200 tokens)
    for year, cnt in per_year_token_occ.items():
        rows = []
        for tok, occ in cnt.most_common():
            rows.append({
                "text": tok,
                "messages_count": len(per_year_token_msgs[year][tok]),
                "occurrences": occ
            })
        rows.sort(key=lambda r: (-int(r["messages_count"]), -int(r["occurrences"]), r["text"]))
        with open(OUT_BY_YEAR_TPL.format(year=year), "w", encoding="utf-8", newline="") as f:
            fn = ["text","messages_count","occurrences"]
            w = csv.DictWriter(f, fieldnames=fn)
            w.writeheader()
            for r in rows[:200]:
                w.writerow(r)

    print(f"✅ Fini. Messages analysés: {total_messages}")
    print("🗂 Sorties :")
    print(f"  - {OUT_ALL}")
    print(f"  - {OUT_ENT}")
    print(f"  - {OUT_CHUNKS}")
    print(f"  - {OUT_BY_YEAR_TPL.replace('{year}', 'YYYY')}")

if __name__ == "__main__":
    main()
