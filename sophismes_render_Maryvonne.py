# sophismes_render.py
# - Analyse chaque email (avec 1 message de contexte avant/après)
# - Génère un HTML complet avec surlignage jaune des extraits incriminés
# - Renseigne un CSV CENTRAL unique avec :
#     id_topic, year, email_id, email_sender, speaker, field, text, status_code
#   où:
#     field ∈ {"point_majeur(0)","excuse_fallacieuse(11)","argument_valide_mal_employe(12)"}
#     status_code ∈ {"01","02"} pour point_majeur(0) (01=open, 02=hidden), sinon ""
#
# Entrée  : relations/Maryvonne/txt_y.mandaba_full1/emails_<YEAR>.txt
# Sorties :
#   - HTML : relations/Maryvonne/txt_y.mandaba_full1/sophismes/emails_<YEAR}_sophismes.html
#   - CSV  : relations/Maryvonne/analysis/sophismes/sophismes_topics_master.csv  (central)
# Comment lister/classer les sophismes ?
#   "off"        : IA libre (pas de taxonomie)
#   "hint"       : on montre la liste au modèle (influence)
#   "normalize"  : on mappe vers YAML si possible (soft, recommandé)
#   "strict"     : on mappe, sinon bascule en "Autre / Non classé"

# sophismes_render.py
# - Analyse chaque e-mail (avec 1 msg de contexte avant/après)
# - Génère un HTML complet avec surlignage jaune (citations des sophismes)
# - Écrit un CSV central unique avec les "topics" (points majeurs, excuses fallacieuses, arguments vrais mal employés)
# - Taxonomie de sophismes chargée depuis YAML, avec modes : off | hint | normalize | strict
#   (par défaut: "hint" → on montre la liste au modèle, sans imposer le mapping)

import os, re, csv, json, time, html, difflib, unicodedata
from datetime import datetime

# ====== .env (clé OPENAI_API_KEY si besoin) ======
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass
# normalize accidental quotes/spaces
k = (os.getenv("OPENAI_API_KEY") or "").strip().strip('"').strip("'")
os.environ["OPENAI_API_KEY"] = k

from openai import OpenAI
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY") or None)

# ===================== CONFIG =====================
YEAR = "2024"
BASE_DIR = "relations/Maryvonne/txt_y.mandaba_full1"
INPUT_FILE = os.path.join(BASE_DIR, f"emails_{YEAR}.txt")

OUT_HTML_DIR = os.path.join(BASE_DIR, "sophismes")
OUTPUT_HTML  = os.path.join(OUT_HTML_DIR, f"emails_{YEAR}_sophismes.html")

OUT_CSV_DIR  = "relations/Maryvonne/analysis/sophismes"
CENTRAL_CSV  = os.path.join(OUT_CSV_DIR, "sophismes_topics_master.csv")

MODEL        = "gpt-4.1-mini"
TEMPERATURE  = 0.1
MAX_MESSAGES = None     # None = tous
CTX_SPAN     = 1        # nb de messages de contexte avant/après
SLEEP_BETWEEN = 0.1     # seconds entre appels

# Taxonomie : "off" | "hint" | "normalize" | "strict"
TAXONOMY_MODE = "hint"

# YAML de taxonomie
TAXO_YAML_PATH = "relations/Maryvonne/config/sophismes.yaml"

# Détection speaker basique via e-mails
MARYVONNE_EMAILS = {
    "maryvonnemm@gmail.com","maryvonne.masseron@free.fr",
    "maryvonnemm@hotmail.com","maryvonnemm@hotmail.fr"
}
YANNICK_EMAILS   = {"y.mandaba@gmail.com","mymmandaba@gmail.com"}


# --- .env + garde-fou clé API ---
import os, re
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

key = os.getenv("OPENAI_API_KEY", "")
# strip guillemets/espace accidentels
key = key.strip().strip('"').strip("'")
os.environ["OPENAI_API_KEY"] = key

def _mask(k: str) -> str:
    if not k: return "MISSING"
    return f"{k[:7]}…{k[-4:]} (len={len(k)})"

# Détection de variables qui perturbent (Azure / base custom)
_sus = {v: os.getenv(v) for v in ["OPENAI_API_BASE","OPENAI_BASE_URL","AZURE_OPENAI_API_KEY","AZURE_OPENAI_ENDPOINT","AZURE_OPENAI_API_VERSION"] if os.getenv(v)}
if _sus:
    print("⚠️ Variables potentiellement conflictuelles détectées:", ", ".join(_sus))

if not key or not key.startswith("sk-") or len(key) < 20:
    raise RuntimeError(f"OPENAI_API_KEY invalide ou absente. Actuelle: {_mask(key)}\n"
                       "→ Mets la vraie clé dans un fichier .env à la racine: OPENAI_API_KEY=sk-…\n"
                       "→ Ou exporte dans le shell: export OPENAI_API_KEY='sk-…'")

# --- fin garde-fou ---

# ===================== OpenAI (API moderne uniquement) =====================
def call_llm(messages, model=MODEL, temperature=TEMPERATURE):
    """
    Utilise le SDK openai >= 1.x (client.chat.completions.create).
    Nécessite OPENAI_API_KEY dans l'environnement.
    """
    try:
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError("Le paquet 'openai' n'est pas installé dans ce venv. "
                           "Installe-le avec: pip install --upgrade openai") from e

    client = OpenAI()
    try:
        r = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        return r.choices[0].message.content
    except Exception as e:
        # On remonte l'erreur avec un message clair
        raise RuntimeError(f"Echec appel OpenAI (API moderne). "
                           f"Vérifie OPENAI_API_KEY et ta connexion réseau. Détail: {e}") from e


# ===================== Utils =====================
def strip_accents(s: str) -> str:
    if not s: return ""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def canon_key(s: str) -> str:
    s = (s or "").lower().strip()
    s = strip_accents(s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def detect_speaker(from_field: str) -> str:
    low = (from_field or "").lower()
    if any(e in low for e in MARYVONNE_EMAILS): return "Maryvonne"
    if any(e in low for e in YANNICK_EMAILS):   return "Yannick"
    return "Autre"

# ===================== Taxonomie YAML =====================
ALIAS_EXTRA = {
    "Hors sujet / diversion": ["hors sujet", "diversion", "off topic", "répond à côté", "répondre à côté", "déviation", "évasion"],
    "Red herring (fausse piste)": ["red herring", "fausse piste", "leurre"],
    "Glissement de sujet": ["changement de sujet", "déplacement du sujet"],
    "Argument d'autorité": ["appel à l'autorité", "autorité", "argument d’autorité"],
    "Appel à la tradition": ["on a toujours fait comme ça", "tradition", "argumentum ad antiquitatem"],
    "Appel au rôle familial": ["bon fils", "bonne mère", "devoir filial", "rôle familial"],
    "Inversion accusatoire": ["inverser l'accusation", "retournement accusatoire"],
    "Projection psychologique": ["projection", "projeter", "attribuer ses défauts"],
    "Contradiction / Mensonge flagrant": ["mensonge", "contradiction", "démenti sans base"],
    "Esquive / absence de réponse": ["éluder", "ne répond pas", "éviter la question"],
    "Demande impossible": ["conditions irréalistes", "inatteignable", "injonction impossible"],
    "Double discours": ["contradictions dans le temps", "discours double"],
    "Argumentation en rafale": ["gish gallop", "tir de barrage", "mitraillage d’arguments"],
    "Confusion volontaire": ["mélanger les faits", "brouiller", "entretenir le flou"],
    "Fausse liste de reproches": ["accusations vagues", "catalogue de reproches", "reproches non étayés"],
}

def load_taxonomy_yaml(path=TAXO_YAML_PATH):
    canon_map = {}
    names_set, cats_set = set(), set()
    try:
        import yaml
        if not os.path.exists(path):
            alt = "sophismes.yaml"
            if os.path.exists(alt):
                path = alt
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or []
            for item in data:
                name = (item.get("name") or "").strip()
                cat  = (item.get("category") or "").strip()
                if not name: 
                    continue
                names_set.add(name)
                if cat: cats_set.add(cat)

                keys = [canon_key(name)]
                for a in (item.get("aliases") or []):
                    if a and a.strip():
                        keys.append(canon_key(a.strip()))
                for a in ALIAS_EXTRA.get(name, []):
                    keys.append(canon_key(a))
                for k in set(keys):
                    if k:
                        canon_map[k] = {"name": name, "category": cat}
    except Exception:
        pass
    return canon_map, names_set, cats_set

CANON_MAP, ALLOWED_NAMES, ALLOWED_CATS = load_taxonomy_yaml()

def normalize_to_taxo(name: str, category: str, canon_map: dict, names_set: set):
    key = canon_key(name)
    hit = canon_map.get(key)
    if hit:
        return hit["name"], (hit["category"] or category or "")
    # fuzzy sur noms canoniques
    choices = list(names_set)
    if choices:
        match = difflib.get_close_matches(name, choices, n=1, cutoff=0.78)
        if match:
            canon_name = match[0]
            canon_hit = canon_map.get(canon_key(canon_name))
            if canon_hit:
                return canon_hit["name"], (canon_hit["category"] or category or "")
    return name, category

# ===================== Parsing emails =====================
EMAIL_BLOCK = re.compile(r"=== MESSAGE ===(.*?)=== FIN ===", re.S)
FIELD_DATE  = re.compile(r"^\s*🗕\s*Date\s*:\s*(.*)$", re.M)
FIELD_FROM  = re.compile(r"^\s*👤\s*From\s*:\s*(.*)$", re.M)
FIELD_TO    = re.compile(r"^\s*📨\s*To\s*:\s*(.*)$", re.M)
FIELD_SUBJ  = re.compile(r"^\s*🧕\s*Subject\s*:\s*(.*)$", re.M)
SEP_LINE    = re.compile(r"^\s*---\s*$", re.M)

def load_messages(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    blocks = [blk.strip() for blk in EMAIL_BLOCK.findall(content)]
    msgs = []
    for b in blocks:
        date = (FIELD_DATE.search(b).group(1).strip() if FIELD_DATE.search(b) else "")
        from_ = (FIELD_FROM.search(b).group(1).strip() if FIELD_FROM.search(b) else "")
        to    = (FIELD_TO.search(b).group(1).strip() if FIELD_TO.search(b) else "")
        subj  = (FIELD_SUBJ.search(b).group(1).strip() if FIELD_SUBJ.search(b) else "")
        body = b
        m = SEP_LINE.search(b)
        if m:
            body = b[m.end():].strip()
        # Décodage quoted-printable minimal (safe)
        body = body.replace("=\n", "").replace("=20", " ")
        msgs.append({"raw": b, "date": date, "from": from_, "to": to, "subject": subj, "body": body})
    return msgs

def summarize_for_ctx(txt, cap=1200):
    if not txt: return ""
    t = txt.strip()
    if len(t) > cap:
        t = t[:cap] + "..."
    return t

# ===================== Highlight helpers =====================
def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _quote_pattern(q: str):
    q = q.strip()
    q = re.sub(r"\s+", " ", q)
    esc = re.escape(q).replace(r"\ ", r"\s+")
    return re.compile(esc, re.I | re.S)

def compute_spans_for_quotes(text: str, quotes):
    taken = [False] * (len(text) + 1)
    spans = []
    for q in (quotes or []):
        if not q: continue
        if len(_normalize_ws(q)) < 6:
            continue
        m = _quote_pattern(q).search(text)
        if not m: continue
        s, e = m.span()
        if any(taken[s:e]): continue
        spans.append((s, e))
        for i in range(s, e): taken[i] = True
    spans.sort()
    merged = []
    for s, e in spans:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            merged[-1][1] = max(merged[-1][1], e)
    return [(s, e) for s, e in merged]

def render_body_with_highlights(body: str, spans):
    parts, last = [], 0
    for s, e in spans:
        if s > last: parts.append(html.escape(body[last:s]))
        parts.append(f'<mark class="hl">{html.escape(body[s:e])}</mark>')
        last = e
    parts.append(html.escape(body[last:]))
    return "".join(parts).replace("\t","    ").replace("\n","<br>\n")

# ===================== PROMPT =====================
SYSTEM_PROMPT = (
    "Tu es un analyste rigoureux des échanges familiaux et des sophismes. "
    "RÉPONDS STRICTEMENT EN JSON VALIDE, SANS TEXTE HORS JSON."
)

USER_TEMPLATE = """
Tu vas analyser UN e-mail (avec un peu de contexte avant/après). Retourne STRICTEMENT du JSON avec la structure ci-dessous.

Objectif (à faire **silencieusement** avant de répondre) :
1) Repérer les sophismes éventuels (chez l'expéditeur OU l'autre partie), nom + catégorie + explication + citations exactes (pour surlignage).
2) Inférer les VRAIS SUJETS ("points majeurs") : ce que Maryvonne veut/demande/insinue, ce que Yannick veut/propose ; noter si c'est OUVERT ou CACHÉ, et pourquoi tu penses que c'est caché (si applicable). Donne des formulations courtes (1–3 phrases par item).
3) Lister :
   - excuses/arguments fallacieux (mauvais arguments)  → speaker + label + 1–3 formulations/expressions courtes + explication
   - arguments valides mais utilisés de manière fallacieuse → speaker + description + 1–3 formulations + explication
4) Donne aussi une courte liste "major_points" (bullets) pour mémoire (pas stockée au CSV).

{taxonomy_hint}

FORMAT JSON EXACT à retourner :

{{
  "sophisms": [
    {{ "name": "...", "category": "Diversion|Manipulation émotionnelle|Autorité ou légitimité|Inversion et falsification|Mauvaise foi|Saturation|...", "explanation": "...",
      "quotes": ["extrait exact", "extrait exact (<=240c)"] }}
  ],
  "real_matters": [
    {{ "speaker": "Maryvonne|Yannick", "open_or_hidden": "open|hidden",
      "phrases": ["phrase courte 1", "phrase courte 2"], "why_hidden": "..." }}
  ],
  "fallacious_excuses": [
    {{ "speaker": "Maryvonne|Yannick", "label": "nom court", "phrases": ["expr/phrase", "…"], "explanation": "..." }}
  ],
  "valid_but_misused": [
    {{ "speaker": "Maryvonne|Yannick", "description": "point vrai mais détourné", "phrases": ["expr/phrase"], "explanation": "..." }}
  ],
  "major_points": ["…","…"]
}}

Si rien n’est détecté dans une section, renvoie-la comme liste vide [].

=== FICHE MESSAGE ===
[FOCUS] Analyse prioritairement le message courant, mais utilise le contexte pour interpréter.
Date : {date}
From : {from_}
To   : {to}
Subj : {subj}

=== CORPS COURANT ===
{body}

=== CONTEXTE AVANT (résumé brut) ===
{ctx_before}

=== CONTEXTE APRÈS (résumé brut) ===
{ctx_after}
"""

def taxonomy_hint_block():
    # affiché uniquement en mode "hint" ou "strict"
    if TAXONOMY_MODE not in ("hint","strict"):
        return ""
    names = ", ".join(sorted(ALLOWED_NAMES)) if ALLOWED_NAMES else "—"
    cats  = ", ".join(sorted(ALLOWED_CATS))  if ALLOWED_CATS else "—"
    return f"Nomenclature de référence (utilise ces libellés si pertinent) :\n- Noms canoniques: {names}\n- Catégories: {cats}\n"

# ===================== LLM wrapper =====================
def analyze_with_context(messages, idx):
    msg = messages[idx]
    before = messages[idx-1]["body"] if idx-1 >= 0 else ""
    after  = messages[idx+1]["body"] if idx+1 < len(messages) else ""
    user = USER_TEMPLATE.format(
        taxonomy_hint=taxonomy_hint_block(),
        date=msg["date"], from_=msg["from"], to=msg["to"], subj=msg["subject"],
        body=msg["body"],
        ctx_before=summarize_for_ctx(before),
        ctx_after=summarize_for_ctx(after)
    )
    messages_payload = [{"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user}]
    raw = call_llm(messages_payload)

    # sécuriser JSON
    data = {"sophisms":[], "real_matters":[], "fallacious_excuses":[], "valid_but_misused":[], "major_points":[]}
    try:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            raw = raw[start:end+1]
        parsed = json.loads(raw)
        for k in data.keys():
            if isinstance(parsed.get(k), list): data[k] = parsed.get(k)
    except Exception:
        pass

    # Normalisation (si demandé)
    if TAXONOMY_MODE in ("normalize","strict"):
        s_norm = []
        for s in data.get("sophisms", []) or []:
            nm = (s.get("name") or "").strip()
            ct = (s.get("category") or "").strip()
            nm2, ct2 = normalize_to_taxo(nm, ct, CANON_MAP, ALLOWED_NAMES)
            mapped = (nm2 != nm)
            if TAXONOMY_MODE == "strict" and not mapped:
                nm2, ct2 = "Autre / Non classé", "Autre"
            s["original_name"] = nm
            s["name"] = nm2
            s["category"] = ct2 or ct
            s_norm.append(s)
        data["sophisms"] = s_norm

    return data

# ===================== HTML render =====================
def build_html(messages, analyses, stats):
    css = """
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color:#222; }
      h1 { font-size: 24px; margin: 0 0 6px; }
      .meta { color:#555; font-size: 13px; margin-bottom: 18px; }
      .summary { padding: 12px 14px; background:#f5f7ff; border:1px solid #e3e8ff; border-radius: 10px; margin-bottom: 20px; }
      .grid { display:grid; grid-template-columns: repeat(4,1fr); gap:10px; }
      .kpi { background:#fff; border:1px solid #eee; border-radius:10px; padding:10px; }
      .kpi .n { font-weight:700; font-size:20px; }
      .msg { border:1px solid #eee; border-radius:12px; margin:18px 0; overflow:hidden; }
      .msg header { background:#fafafa; border-bottom:1px solid #eee; padding:10px 12px; }
      .msg header .line { font-size:13px; color:#444; }
      .msg .body { padding:14px 12px; line-height:1.5; background:#fff; }
      .msg .rapport { border-top:1px dashed #e5e5e5; padding:10px 12px; background:#fffef6; }
      .msg .rapport h4 { margin:0 0 8px 0; }
      .msg .rapport .item { padding:8px 10px; border:1px solid #f0e3a5; background:#fffbe0; border-radius:8px; margin:6px 0; }
      .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; background:#eef; border:1px solid #dde; margin-right:6px; }
      .hl { background: #fff34d; }
      .muted { color:#777; font-size:13px; }
      .empty { color:#aaa; font-style: italic; }
      @media (max-width:900px){ .grid{grid-template-columns:1fr 1fr;} }
      @media (max-width:560px){ .grid{grid-template-columns:1fr;} }
    </style>
    """
    header = f"<h1>Analyse des sophismes — {YEAR}</h1><div class='meta'>Mode taxonomie: {html.escape(TAXONOMY_MODE)} · Généré le {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>"
    cat_items = "".join(
        f"<div class='kpi'><div class='n'>{n}</div><div class='muted'>{html.escape(c)}</div></div>"
        for c, n in sorted(stats['by_category'].items(), key=lambda x: (-x[1], x[0]))
    ) or "<div class='muted'>—</div>"
    summary = f"""
    <div class="summary">
      <div class="grid">
        <div class='kpi'><div class='n'>{stats['total_messages']}</div><div class='muted'>Messages</div></div>
        <div class='kpi'><div class='n'>{stats['messages_with_sophisms']}</div><div class='muted'>Avec sophismes</div></div>
        <div class='kpi'><div class='n'>{stats['total_sophisms']}</div><div class='muted'>Sophismes détectés</div></div>
        <div class='kpi'><div class='n'>{stats['unique_topics_count']}</div><div class='muted'>Topics (CSV)</div></div>
      </div>
      <div style="margin-top:10px;">
        <b>Par catégorie :</b>
        <div class="grid" style="margin-top:8px;">{cat_items}</div>
      </div>
    </div>
    """
    sections = []
    for i, (msg, data) in enumerate(zip(messages, analyses), start=1):
        quotes = []
        for s in data.get("sophisms", []):
            for q in (s.get("quotes") or []):
                if q and q.strip(): quotes.append(q.strip())
        spans = compute_spans_for_quotes(msg["body"], quotes)
        body_html = render_body_with_highlights(msg["body"], spans)

        # rapport
        blocks = []
        sophs = data.get("sophisms") or []
        if sophs:
            for s in sophs:
                name_txt = (s.get("name") or "—")
                orig = s.get("original_name")
                name = html.escape(name_txt)
                if orig and orig.strip() and orig.strip() != name_txt.strip():
                    name += f" <span class='muted'>(orig: {html.escape(orig)})</span>"
                cat  = html.escape((s.get("category") or "—"))
                exp  = html.escape((s.get("explanation") or ""))
                qts  = s.get("quotes") or []
                qhtml = "".join(f"<li>« {html.escape(q)} »</li>" for q in qts[:3])
                blocks.append(f"""
                  <div class="item">
                    <div><span class="badge">{cat}</span><b>{name}</b></div>
                    <div class="muted" style="margin:6px 0;">{exp}</div>
                    {('<div><b>Extraits :</b><ul>'+qhtml+'</ul></div>') if qhtml else ''}
                  </div>
                """)
        rm = data.get("real_matters") or []
        if rm:
            items = []
            for r in rm:
                spk = html.escape(r.get("speaker") or "—")
                oh  = html.escape(r.get("open_or_hidden") or "—")
                phs = r.get("phrases") or []
                why_raw = r.get("why_hidden") or ""
                why_html = f' <span class="muted">({html.escape(why_raw)})</span>' if why_raw else ""
                items.append(f"<li><b>{spk}</b> — <i>{oh}</i> : {html.escape('; '.join(phs))}{why_html}</li>")
            blocks.append(f"<div class='item'><div><b>Vrais sujets</b></div><ul>{''.join(items)}</ul></div>")

        fe = data.get("fallacious_excuses") or []
        if fe:
            items = []
            for r in fe:
                spk = html.escape(r.get("speaker") or "—")
                lab = html.escape(r.get("label") or "—")
                phs = r.get("phrases") or []
                exp = html.escape(r.get("explanation") or "")
                items.append(f"<li><b>{spk}</b> — {lab}: {html.escape('; '.join(phs))} <span class='muted'>{exp}</span></li>")
            blocks.append(f"<div class='item'><div><b>Excuses/arguments fallacieux</b></div><ul>{''.join(items)}</ul></div>")

        vm = data.get("valid_but_misused") or []
        if vm:
            items = []
            for r in vm:
                spk = html.escape(r.get("speaker") or "—")
                desc= html.escape(r.get("description") or "—")
                phs = r.get("phrases") or []
                exp = html.escape(r.get("explanation") or "")
                items.append(f"<li><b>{spk}</b> — {desc}: {html.escape('; '.join(phs))} <span class='muted'>{exp}</span></li>")
            blocks.append(f"<div class='item'><div><b>Arguments vrais mais mal employés</b></div><ul>{''.join(items)}</ul></div>")

        rapport_html = ("<div class='rapport'><h4>Rapport sophismes</h4>"+ "".join(blocks) +"</div>") if blocks else "<div class='rapport'><span class='empty'>Aucun sophisme détecté.</span></div>"

        meta_html = f"""
        <div class="line"><b>Date</b> : {html.escape(msg['date'] or '')}</div>
        <div class="line"><b>From</b> : {html.escape(msg['from'] or '')}</div>
        <div class="line"><b>To</b>   : {html.escape(msg['to'] or '')}</div>
        <div class="line"><b>Subj</b> : {html.escape(msg['subject'] or '')}</div>
        """
        sections.append(f"""
        <section class="msg">
          <header>
            <div><b>Message #{i}</b></div>
            {meta_html}
          </header>
          <div class="body">{body_html}</div>
          {rapport_html}
        </section>
        """)
    html_doc = f"<!doctype html><html><head><meta charset='utf-8'><title>Sophismes {YEAR}</title>{css}</head><body>{header}{summary}{''.join(sections)}</body></html>"
    return html_doc

# ===================== CSV central =====================
CSV_FIELDS = ["id_topic","year","email_id","email_sender","speaker","field","text","status_code"]

def ensure_csv_header(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()

def load_existing_ids(path):
    if not os.path.exists(path): return set()
    ids = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                ids.add(r.get("id_topic"))
    except Exception:
        pass
    return ids

def write_rows_append(path, rows):
    if not rows: return
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        for r in rows: w.writerow(r)

def new_topic_id(year, msg_idx, local_idx):
    return f"Y{year}-{msg_idx:05d}-{local_idx:02d}"

# ===================== MAIN =====================
def main():
    os.makedirs(OUT_HTML_DIR, exist_ok=True)
    os.makedirs(OUT_CSV_DIR, exist_ok=True)
    ensure_csv_header(CENTRAL_CSV)
    existing = load_existing_ids(CENTRAL_CSV)

    messages = load_messages(INPUT_FILE)
    if MAX_MESSAGES: messages = messages[:MAX_MESSAGES]

    analyses = []
    cat_counter = {}
    topic_rows = []
    unique_topic_ids = set()

    total = len(messages)
    print(f"✉️  {total} messages… | mode taxonomie: {TAXONOMY_MODE}")

    for i in range(total):
        print(f"➡️  Message {i+1}/{total}")
        data = analyze_with_context(messages, i)
        analyses.append(data)

        # Stats catégories via sophisms
        s_list = data.get("sophisms") or []
        for s in s_list:
            cat = (s.get("category") or "—").strip()
            cat_counter[cat] = cat_counter.get(cat, 0) + 1

        # CSV topics
        email_id     = f"{YEAR}:{i+1}"
        email_sender = messages[i]["from"]
        local_idx = 1

        # real_matters → point_majeur(0)
        for rm in (data.get("real_matters") or []):
            speaker = rm.get("speaker") or ""
            open_hidden = (rm.get("open_or_hidden") or "").lower()
            status_code = "01" if open_hidden == "open" else ("02" if open_hidden == "hidden" else "")
            for ph in (rm.get("phrases") or []):
                tid = new_topic_id(YEAR, i+1, local_idx); local_idx += 1
                if tid in existing or tid in unique_topic_ids: 
                    continue
                topic_rows.append({
                    "id_topic": tid,
                    "year": YEAR,
                    "email_id": email_id,
                    "email_sender": email_sender,
                    "speaker": speaker or detect_speaker(email_sender),
                    "field": "point_majeur(0)",
                    "text": ph.strip(),
                    "status_code": status_code
                })
                unique_topic_ids.add(tid)

        # fallacious_excuses(11)
        for fe in (data.get("fallacious_excuses") or []):
            speaker = fe.get("speaker") or ""
            for ph in (fe.get("phrases") or []):
                tid = new_topic_id(YEAR, i+1, local_idx); local_idx += 1
                if tid in existing or tid in unique_topic_ids: 
                    continue
                topic_rows.append({
                    "id_topic": tid,
                    "year": YEAR,
                    "email_id": email_id,
                    "email_sender": email_sender,
                    "speaker": speaker or detect_speaker(email_sender),
                    "field": "excuse_fallacieuse(11)",
                    "text": ph.strip(),
                    "status_code": ""
                })
                unique_topic_ids.add(tid)

        # valid_but_misused(12)
        for vm in (data.get("valid_but_misused") or []):
            speaker = vm.get("speaker") or ""
            for ph in (vm.get("phrases") or []):
                tid = new_topic_id(YEAR, i+1, local_idx); local_idx += 1
                if tid in existing or tid in unique_topic_ids: 
                    continue
                topic_rows.append({
                    "id_topic": tid,
                    "year": YEAR,
                    "email_id": email_id,
                    "email_sender": email_sender,
                    "speaker": speaker or detect_speaker(email_sender),
                    "field": "argument_valide_mal_employe(12)",
                    "text": ph.strip(),
                    "status_code": ""
                })
                unique_topic_ids.add(tid)

        if SLEEP_BETWEEN: time.sleep(SLEEP_BETWEEN)

    write_rows_append(CENTRAL_CSV, topic_rows)

    stats = {
        "total_messages": len(messages),
        "messages_with_sophisms": sum(1 for d in analyses if (d.get("sophisms") or d.get("real_matters") or d.get("fallacious_excuses") or d.get("valid_but_misused"))),
        "total_sophisms": sum(len(d.get("sophisms") or []) for d in analyses),
        "by_category": cat_counter,
        "unique_topics_count": len(topic_rows) + len(existing)
    }
    html_doc = build_html(messages, analyses, stats)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_doc)

    print(f"✅ HTML : {OUTPUT_HTML}")
    print(f"✅ CSV central (append) : {CENTRAL_CSV}")
    print(f"➕ Lignes ajoutées : {len(topic_rows)}")

if __name__ == "__main__":
    main()
