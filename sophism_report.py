#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sophism_report.py

- Analyse chaque e-mail (avec un peu de contexte avant/après)
- Génère un HTML complet avec surlignage jaune (citations de sophismes)
- Écrit un CSV léger + un CSV d'événements enrichi
- Fonctionne pour n'importe quelle relation définie dans config/autopn.yaml
"""

import argparse
import csv
import difflib
import html
import json
import os
import re
import time
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from autopn.config import (
    expand_path,
    load_config,
    openai_settings,
    owner_emails,
    owner_label,
    relation_context,
    relation_emails,
    relation_label,
    relation_theme_keywords,
    resolve_relation,
)
from autopn.events import EventsSink, guess_theme

RUN_CONTEXT: Dict[str, Any] = {}

try:
    from dotenv import find_dotenv, load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None
    find_dotenv = None


def _mask(key: str) -> str:
    if not key:
        return "MISSING"
    return f"{key[:7]}…{key[-4:]} (len={len(key)})"


def ensure_openai_api_key() -> None:
    if load_dotenv and find_dotenv:
        load_dotenv(find_dotenv(usecwd=True))
    key = (os.getenv("OPENAI_API_KEY") or "").strip().strip('"').strip("'")
    os.environ["OPENAI_API_KEY"] = key
    if not key or not key.startswith("sk-") or len(key) < 20:
        raise RuntimeError(
            "OPENAI_API_KEY manquante ou invalide "
            f"(actuelle: {_mask(key)}). Ajoute-la dans .env ou l'environnement."
        )


def call_llm(messages, *, model: str, temperature: float):
    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - import guard
        raise RuntimeError("Le paquet 'openai' n'est pas installé. pip install --upgrade openai") from exc
    client = OpenAI()
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        return response.choices[0].message.content
    except Exception as exc:
        raise RuntimeError(f"Echec appel OpenAI: {exc}") from exc


EMAIL_BLOCK = re.compile(r"(?ms)^=== MESSAGE ===\n.*?\n=== FIN ===")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a sophism report for one relation/year.")
    parser.add_argument("--config", help="Path to config/autopn.yaml (defaults to env or repo copy).")
    parser.add_argument("--relation", help="Relation id defined in the config.")
    parser.add_argument("--year", type=int, help="Single year to process (e.g., 2022).")
    parser.add_argument(
        "--years",
        type=str,
        help='Multiple years: "all", "YYYY-YYYY", or "YYYY,YYYY,YYYY".',
    )
    parser.add_argument("--base-dir", help="Override folder containing emails_YYYY.txt.")
    parser.add_argument("--normalized", action="store_true", help="Read emails_YYYY_NORMALIZED.txt files.")
    parser.add_argument("--clear-light", action="store_true", help="Truncate the light CSV before running.")
    parser.add_argument("--clear-events", action="store_true", help="Recreate the events CSV before running.")
    parser.add_argument("--max", type=int, default=None, help="Cap the number of messages per year.")
    parser.add_argument(
        "--sleep",
        type=float,
        help="Delay (in seconds) between LLM calls. Defaults to config.openai.sleep_between_requests.",
    )
    parser.add_argument(
        "--taxonomy-mode",
        choices=["off", "hint", "normalize", "strict"],
        default="hint",
        help="How strictly to enforce the taxonomy names.",
    )
    parser.add_argument(
        "--taxonomy-file",
        default="sophismes.yaml",
        help="Path to the taxonomy YAML file.",
    )
    parser.add_argument(
        "--model",
        help="Override the OpenAI model (defaults to config.openai.model).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        help="Override sampling temperature (defaults to config.openai.temperature).",
    )
    return parser.parse_args()

def discover_years(base_dir: Path, normalized: bool = False) -> List[int]:
    patt = "emails_*_NORMALIZED.txt" if normalized else "emails_*.txt"
    years = set()
    for path in base_dir.glob(patt):
        match = re.search(r"emails_(\d{4})", path.name)
        if match:
            years.add(int(match.group(1)))
    return sorted(years)


def resolve_years_arg(args: argparse.Namespace, base_dir: Path) -> List[int]:
    if args.years:
        token = args.years.strip().lower()
        if token == "all":
            years = discover_years(base_dir, args.normalized)
            if not years:
                raise FileNotFoundError(f"Aucun emails_YYYY*.txt trouvé dans {base_dir}")
            return years
        if "-" in token:
            a, b = token.split("-", 1)
            start, end = int(a), int(b)
            span = range(min(start, end), max(start, end) + 1)
            return list(span)
        if "," in token:
            return [int(y) for y in token.split(",") if y.strip()]
        return [int(token)]
    if args.year:
        return [args.year]
    years = discover_years(base_dir, args.normalized)
    if not years:
        raise FileNotFoundError(f"Aucune année n'a été trouvée dans {base_dir}")
    return years



def strip_accents(s: str) -> str:
    if not s: return ""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def canon_key(s: str) -> str:
    s = (s or "").lower().strip()
    s = strip_accents(s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def build_speaker_detector(owner_addresses: List[str], relation_addresses: List[str],
                           owner_name: str, relation_name: str):
    owner_tokens = [addr.lower() for addr in owner_addresses]
    relation_tokens = [addr.lower() for addr in relation_addresses]

    def detect(from_field: str) -> str:
        low = (from_field or "").lower()
        if any(token in low for token in relation_tokens):
            return relation_name
        if any(token in low for token in owner_tokens):
            return owner_name
        return "Autre"

    return detect

def extract_around(needle: str, text: str, window: int = 140) -> str:
    if not needle or not text:
        return ""
    t_low = text.lower()
    n_low = needle.lower().strip()
    i = t_low.find(n_low)
    if i == -1:
        return ""
    start = max(0, i - window)
    end   = min(len(text), i + len(n_low) + window)
    return re.sub(r"\s+", " ", text[start:end]).strip()

# ===================== Taxonomie YAML =====================
ALIAS_EXTRA = {
    "Hors sujet / diversion": ["hors sujet", "diversion", "off topic", "répond à côté", "déviation", "évasion"],
    "Red herring (fausse piste)": ["red herring", "fausse piste", "leurre"],
    "Glissement de sujet": ["changement de sujet", "déplacement du sujet"],
    "Argument d'autorité": ["appel à l'autorité", "autorité", "argument d’autorité"],
    "Appel à la tradition": ["on a toujours fait comme ça", "tradition"],
    "Appel au rôle familial": ["bon fils", "bonne mère", "devoir filial", "rôle familial"],
    "Inversion accusatoire": ["retournement accusatoire"],
    "Projection psychologique": ["projection"],
    "Contradiction / Mensonge flagrant": ["mensonge", "contradiction"],
    "Esquive / absence de réponse": ["éluder", "ne répond pas", "éviter la question"],
    "Demande impossible": ["conditions irréalistes", "inatteignable"],
    "Double discours": ["contradictions dans le temps"],
    "Argumentation en rafale": ["gish gallop","tir de barrage"],
    "Confusion volontaire": ["mélanger les faits","brouiller"],
    "Fausse liste de reproches": ["accusations vagues","catalogue de reproches"],
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
                data = (yaml.safe_load(f) or [])
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

def normalize_to_taxo(name: str, category: str, canon_map: dict, names_set: set):
    key = canon_key(name)
    hit = canon_map.get(key)
    if hit:
        return hit["name"], (hit["category"] or category or "")
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
Analyse UN e-mail avec un peu de contexte. Retourne STRICTEMENT du JSON au format ci-dessous.

Objectif (réflexion silencieuse d’abord) :
1) Repérer les sophismes (expéditeur OU interlocuteur) : nom + catégorie + explication + citations EXACTES (<=240c).
2) Inférer les VRAIS SUJETS (“real_matters”) : ce que {relation_name} veut/demande/insinue, ce que {owner_name} veut/propose ;
   note OPEN vs HIDDEN et “why_hidden” si caché. Donne 1–3 formulations courtes par item.
3) Lister :
   - excuses/arguments fallacieux (mauvais arguments)  → speaker + label + 1–3 expressions + explication
   - arguments valides mais mal employés → speaker + description + 1–3 expressions + explication
4) Ajouter “major_points” (quelques bullets pour mémoire).

{taxonomy_hint}

FORMAT JSON EXACT :

{{
  "sophisms": [
    {{ "name": "...", "category": "Diversion|Manipulation émotionnelle|Autorité ou légitimité|Inversion et falsification|Mauvaise foi|Saturation|...", "explanation": "...",
      "quotes": ["extrait exact", "extrait exact (<=240c)"] }}
  ],
  "real_matters": [
    {{ "speaker": "{relation_name}|{owner_name}", "open_or_hidden": "open|hidden",
      "phrases": ["phrase courte 1", "phrase courte 2"], "why_hidden": "..." }}
  ],
  "fallacious_excuses": [
    {{ "speaker": "{relation_name}|{owner_name}", "label": "nom court", "phrases": ["expr/phrase", "…"], "explanation": "..." }}
  ],
  "valid_but_misused": [
    {{ "speaker": "{relation_name}|{owner_name}", "description": "point vrai mais détourné", "phrases": ["expr/phrase"], "explanation": "..." }}
  ],
  "major_points": ["…","…"]
}}

Si rien n’est détecté dans une section, renvoie [].

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

=== CONTEXTE RELATIONNEL ===
{relationship_context}

=== CONTEXTE APRÈS (résumé brut) ===
{ctx_after}
"""



def taxonomy_hint_block() -> str:
    mode = RUN_CONTEXT.get("taxonomy_mode", "off")
    if mode not in ("hint", "strict"):
        return ""
    allowed_names = RUN_CONTEXT.get("allowed_names") or []
    allowed_cats = RUN_CONTEXT.get("allowed_cats") or []
    names = ", ".join(sorted(allowed_names)) if allowed_names else "—"
    cats = ", ".join(sorted(allowed_cats)) if allowed_cats else "—"
    return (
        "Nomenclature de référence (utilise ces libellés si pertinent) :\n"
        f"- Noms canoniques: {names}\n- Catégories: {cats}\n"
    )

# ===================== LLM wrapper =====================
def analyze_with_context(messages, idx):
    ctx = RUN_CONTEXT
    msg = messages[idx]
    before = messages[idx - 1]["body"] if idx - 1 >= 0 else ""
    after = messages[idx + 1]["body"] if idx + 1 < len(messages) else ""
    user = USER_TEMPLATE.format(
        taxonomy_hint=taxonomy_hint_block(),
        date=msg["date"],
        from_=msg["from"],
        to=msg["to"],
        subj=msg["subject"],
        body=msg["body"],
        ctx_before=summarize_for_ctx(before),
        ctx_after=summarize_for_ctx(after),
        owner_name=ctx.get("owner_name", "Owner"),
        relation_name=ctx.get("relation_name", "Relation"),
        relationship_context=ctx.get("relation_context", "—") or "—",
    )
    messages_payload = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
    raw = call_llm(
        messages_payload,
        model=ctx.get("model", "gpt-4o-mini"),
        temperature=ctx.get("temperature", 0.6),
    )

    # sécuriser JSON
    data = {"sophisms":[], "real_matters":[], "fallacious_excuses":[], "valid_but_misused":[], "major_points":[]}
    try:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            raw = raw[start:end+1]
        parsed = json.loads(raw)
        for k in data.keys():
            if isinstance(parsed.get(k), list):
                data[k] = parsed.get(k)
    except Exception:
        pass

    taxonomy_mode = ctx.get("taxonomy_mode", "off")
    canon_map = ctx.get("canon_map") or {}
    allowed_names = ctx.get("allowed_names") or set()
    # Normalisation (si demandé)
    if taxonomy_mode in ("normalize","strict"):
        s_norm = []
        for s in data.get("sophisms", []) or []:
            nm = (s.get("name") or "").strip()
            ct = (s.get("category") or "").strip()
            nm2, ct2 = normalize_to_taxo(nm, ct, canon_map, allowed_names)
            mapped = (nm2 != nm)
            if taxonomy_mode == "strict" and not mapped:
                nm2, ct2 = "Autre / Non classé", "Autre"
            s["original_name"] = nm
            s["name"] = nm2
            s["category"] = ct2 or ct
            s_norm.append(s)
        data["sophisms"] = s_norm

    return data

# ===================== HTML render =====================
def build_html(messages, analyses, stats, year):
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
    relation_name = RUN_CONTEXT.get("relation_name", "Relation")
    mode = RUN_CONTEXT.get("taxonomy_mode", "hint")
    header = (
        f"<h1>Analyse des sophismes — {html.escape(relation_name)} — {year}</h1>"
        f"<div class='meta'>Mode taxonomie: {html.escape(mode)} · "
        f"Généré le {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>"
    )
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
    html_doc = f"<!doctype html><html><head><meta charset='utf-8'><title>Sophismes {year}</title>{css}</head><body>{header}{summary}{''.join(sections)}</body></html>"
    return html_doc

# ===================== CSV (léger) =====================
CSV_FIELDS = ["id_topic","year","email_id","email_sender","speaker","field","text","status_code"]

def ensure_csv_header(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()

def load_existing_ids(path: Path):
    if not path.exists(): return set()
    ids = set()
    try:
        with path.open("r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                ids.add(r.get("id_topic"))
    except Exception:
        pass
    return ids

def write_rows_append(path: Path, rows):
    if not rows: return
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        for r in rows: w.writerow(r)

def new_topic_id(year, msg_idx, local_idx):
    return f"Y{year}-{msg_idx:05d}-{local_idx:02d}"

def process_one_year(year: int, in_path: Path, out_html_dir: Path, max_messages: Optional[int]):
    ctx = RUN_CONTEXT
    taxonomy_mode = ctx.get("taxonomy_mode", "hint")
    sleep_between = ctx.get("sleep_between", 0.15)
    events_sink: EventsSink = ctx["events_sink"]
    central_csv: Path = ctx["central_csv"]
    speaker_detector = ctx.get("speaker_detector")
    theme_keywords = ctx.get("theme_keywords")

    out_html_dir.mkdir(parents=True, exist_ok=True)
    ensure_csv_header(central_csv)
    existing = load_existing_ids(central_csv)

    messages = load_messages(in_path)
    if max_messages:
        messages = messages[:max_messages]

    analyses: List[Dict[str, Any]] = []
    cat_counter: Counter = Counter()
    topic_rows: List[Dict[str, Any]] = []
    unique_topic_ids = set()

    total = len(messages)
    relation_name = ctx.get("relation_name", "Relation")
    print(f"✉️  {total} messages | année {year} | relation {relation_name} | mode taxo {taxonomy_mode}")

    for i in range(total):
        print(f"➡️  Message {i+1}/{total}")
        data = analyze_with_context(messages, i)
        analyses.append(data)

        msg = messages[i]
        email_id = f"{year}:{i+1}"
        date_iso = (msg.get("date") or "").strip() or datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        theme = guess_theme(msg.get("body",""), theme_keywords)
        speaker_from_header = speaker_detector(msg.get("from","")) if speaker_detector else "Autre"
        body_text = msg.get("body","")

        # (1) Sophisms
        for s in (data.get("sophisms") or []):
            nm = (s.get("name") or "").strip()
            cat = (s.get("category") or "").strip()
            for q in (s.get("quotes") or [])[:3]:
                events_sink.write(
                    email_id=email_id,
                    date_iso=date_iso,
                    speaker_name=speaker_from_header,
                    speaker_email=msg.get("from",""),
                    theme=theme,
                    topic_name=nm or "—",
                    topic_side="",
                    topic_visibility="",
                    hidden_topic_hint="",
                    hidden_topic_confidence="",
                    argument_ref_name="",
                    argument_text="",
                    reasoning_name="",
                    sophism_name=nm,
                    sophism_category=cat,
                    fact_texts="",
                    statement_type="",
                    will_texts="",
                    complaint_object="",
                    complaint_strength="",
                    email_excerpt=extract_around(q, body_text),
                    relevance=0.25,
                    reasoning_credibility=0.25,
                    impact_score=0.25,
                    impact_direction="positive",
                )

        # (2) Real matters
        for rm in (data.get("real_matters") or []):
            spk = (rm.get("speaker") or "").strip() or speaker_from_header
            vis = (rm.get("open_or_hidden") or "open").lower()
            why = (rm.get("why_hidden") or "").strip()
            for ph in (rm.get("phrases") or []):
                events_sink.write(
                    email_id=email_id,
                    date_iso=date_iso,
                    speaker_name=spk,
                    speaker_email=msg.get("from",""),
                    theme=theme,
                    topic_name=ph,
                    topic_side="",
                    topic_visibility=vis,
                    hidden_topic_hint=why if vis == "hidden" else "",
                    hidden_topic_confidence=0.60 if vis == "hidden" else "",
                    argument_ref_name="",
                    argument_text="",
                    reasoning_name="",
                    sophism_name="",
                    sophism_category="",
                    fact_texts="",
                    statement_type="",
                    will_texts="",
                    complaint_object="",
                    complaint_strength="",
                    email_excerpt=extract_around(ph, body_text),
                    relevance=0.50,
                    reasoning_credibility=0.50,
                    impact_score=0.50,
                    impact_direction="positive",
                )

        # (3) Fallacious excuses
        for fe in (data.get("fallacious_excuses") or []):
            spk = (fe.get("speaker") or "").strip() or speaker_from_header
            lab = (fe.get("label") or "").strip()
            for ph in (fe.get("phrases") or []):
                events_sink.write(
                    email_id=email_id,
                    date_iso=date_iso,
                    speaker_name=spk,
                    speaker_email=msg.get("from",""),
                    theme=theme,
                    topic_name=lab or ph,
                    topic_side="con",
                    topic_visibility="",
                    hidden_topic_hint="",
                    hidden_topic_confidence="",
                    argument_ref_name=lab or ph,
                    argument_text=ph,
                    reasoning_name="",
                    sophism_name=lab,
                    sophism_category=fe.get("label", ""),
                    fact_texts="",
                    statement_type="",
                    will_texts="",
                    complaint_object="",
                    complaint_strength="",
                    email_excerpt=extract_around(ph, body_text),
                    relevance=0.45,
                    reasoning_credibility=0.40,
                    impact_score=0.40,
                    impact_direction="negative",
                )

        # (4) Valid but misused
        for vm in (data.get("valid_but_misused") or []):
            spk = (vm.get("speaker") or "").strip() or speaker_from_header
            desc = (vm.get("description") or "").strip()
            for ph in (vm.get("phrases") or []):
                events_sink.write(
                    email_id=email_id,
                    date_iso=date_iso,
                    speaker_name=spk,
                    speaker_email=msg.get("from",""),
                    theme=theme,
                    topic_name=desc or ph,
                    topic_side="pro",
                    topic_visibility="",
                    hidden_topic_hint="",
                    hidden_topic_confidence="",
                    argument_ref_name=desc or ph,
                    argument_text=ph,
                    reasoning_name="",
                    sophism_name="",
                    sophism_category="",
                    fact_texts="",
                    statement_type="",
                    will_texts="",
                    complaint_object="",
                    complaint_strength="",
                    email_excerpt=extract_around(ph, body_text),
                    relevance=0.40,
                    reasoning_credibility=0.55,
                    impact_score=0.35,
                    impact_direction="positive",
                )

        # Stats
        for s in (data.get("sophisms") or []):
            cat = (s.get("category") or "Autre").strip() or "Autre"
            cat_counter[cat] += 1

        # Light CSV rows
        email_sender = msg.get("from", "")
        local_idx = 1
        for block, field in (
            (data.get("sophisms"), "sophism"),
            (data.get("real_matters"), "real_matter"),
            (data.get("fallacious_excuses"), "fallacious_excuse"),
            (data.get("valid_but_misused"), "valid_but_misused"),
        ):
            for entry in (block or []):
                texts = entry.get("quotes") or entry.get("phrases") or []
                speaker = entry.get("speaker") or speaker_from_header
                for content in texts:
                    tid = new_topic_id(year, i + 1, local_idx)
                    if tid not in unique_topic_ids and tid not in existing:
                        topic_rows.append(
                            {
                                "id_topic": tid,
                                "year": str(year),
                                "email_id": email_id,
                                "email_sender": email_sender,
                                "speaker": speaker or (speaker_detector(email_sender) if speaker_detector else "Autre"),
                                "field": field,
                                "text": content.strip(),
                                "status_code": "",
                            }
                        )
                        unique_topic_ids.add(tid)
                        local_idx += 1

        if sleep_between:
            time.sleep(sleep_between)

    write_rows_append(central_csv, topic_rows)

    stats = {
        "total_messages": len(messages),
        "messages_with_sophisms": sum(
            1
            for d in analyses
            if (d.get("sophisms") or d.get("real_matters") or d.get("fallacious_excuses") or d.get("valid_but_misused"))
        ),
        "total_sophisms": sum(len(d.get("sophisms") or []) for d in analyses),
        "by_category": cat_counter,
        "unique_topics_count": len(topic_rows) + len(existing),
    }
    out_html_path = out_html_dir / f"emails_{year}_sophismes.html"
    html_doc = build_html(messages, analyses, stats, year)
    out_html_path.write_text(html_doc, encoding="utf-8")

    print(f"✅ HTML : {out_html_path}")
    print(f"✅ EVENTS CSV enrichi : {events_sink.path}")
    print(f"✅ CSV léger : {central_csv}")
    print(f"➕ Lignes légères ajoutées : {len(topic_rows)}")


# ===================== MAIN =====================
def main():
    args = parse_args()
    ensure_openai_api_key()

    config = load_config(args.config)
    relation = resolve_relation(config, args.relation)
    owner_name = owner_label(config)
    relation_name = relation_label(relation)
    owner_addrs = owner_emails(config)
    relation_addrs = relation_emails(relation)
    speaker_detector = build_speaker_detector(owner_addrs, relation_addrs, owner_name, relation_name)

    archives_cfg = relation.get("email_archives") or {}
    base_dir_value = args.base_dir or archives_cfg.get("dir")
    if not base_dir_value:
        raise ValueError("Missing email_archives.dir in config (or override with --base-dir).")
    base_dir = expand_path(base_dir_value)
    base_dir.mkdir(parents=True, exist_ok=True)

    reports_cfg = relation.get("reports") or {}
    html_dir = expand_path(reports_cfg.get("html_dir")) if reports_cfg.get("html_dir") else base_dir / "sophismes"
    central_csv = expand_path(reports_cfg.get("csv_light")) if reports_cfg.get("csv_light") else base_dir / "sophismes_topics_master_light.csv"
    events_csv = expand_path(reports_cfg.get("csv_events")) if reports_cfg.get("csv_events") else base_dir / "sophismes_topics_master.csv"

    if args.clear_light and central_csv.exists():
        central_csv.unlink()
    ensure_csv_header(central_csv)

    if args.clear_events and events_csv.exists():
        events_csv.unlink()
    events_sink = EventsSink(events_csv)

    openai_cfg = openai_settings(config)
    sleep_between = args.sleep if args.sleep is not None else openai_cfg.get("sleep_between_requests", 0.15)
    model = args.model or openai_cfg.get("model")
    temperature = args.temperature if args.temperature is not None else openai_cfg.get("temperature", 0.6)

    taxonomy_file = expand_path(args.taxonomy_file)
    canon_map, allowed_names, allowed_cats = load_taxonomy_yaml(str(taxonomy_file))

    RUN_CONTEXT.update({
        "owner_name": owner_name,
        "relation_name": relation_name,
        "relation_context": relation_context(relation),
        "model": model,
        "temperature": temperature,
        "sleep_between": sleep_between,
        "taxonomy_mode": args.taxonomy_mode,
        "canon_map": canon_map,
        "allowed_names": allowed_names,
        "allowed_cats": allowed_cats,
        "theme_keywords": relation_theme_keywords(relation),
        "events_sink": events_sink,
        "central_csv": central_csv,
        "speaker_detector": speaker_detector,
    })

    years = resolve_years_arg(args, base_dir)
    print(f"🗓  Years to process: {years}")

    suffix = "_NORMALIZED" if args.normalized else ""
    for year in years:
        in_path = base_dir / f"emails_{year}{suffix}.txt"
        if not in_path.exists():
            print(f"⚠️  Skipping {year}: {in_path} introuvable")
            continue
        process_one_year(year, in_path, html_dir, args.max)


if __name__ == "__main__":
    main()
