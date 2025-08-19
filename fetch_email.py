#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# fetch_emails.py — FETCH ONLY (sans logique ENJEU)
# Usage:
#   python3 fetch_emails.py           -> export incrémental
#   python3 fetch_emails.py --reset   -> vide emails_*.txt + _processed_ids.txt + _error_ids.txt
#
# Mises à jour (2025-08-19) :
# - Auto-détection du dossier spécial \All (“Tous les messages”) quel que soit la langue/label.
# - Recherche rapide GMail via X-GM-RAW (une seule requête par correspondant).
# - FETCH groupé par lots (BODY.PEEK[TEXT]) -> énorme gain de vitesse.
# - Idempotence par UID (stables) dans _processed_ids.txt pour éviter tout doublon.
# - Logs clairs: dossier IMAP sélectionné, dossier de sortie, compteurs, UID périodiques.
# - Robustesse: safe uid search/fetch (réessaie sur EOF), bornes d’années respectées.

import imaplib, email, os, re, sys, time
from email.header import decode_header

# === CONFIG ===
IMAP_SERVER    = "imap.gmail.com"
IMAP_FOLDER    = ""  # vide = auto; sinon "INBOX" ou "[Gmail]/All Mail" / "[Gmail]/Tous les messages"
EMAIL_ACCOUNT  = "y.mandaba@gmail.com"
EMAIL_PASSWORD = "jdkm edch bdxa osxw"  # App Password si 2FA

RELATION_ROOT = "relations/Maryvonne/txt_y.mandaba"   # adapte si besoin
DEST_FOLDER   = RELATION_ROOT

IDS_LOG_FILE    = os.path.join(DEST_FOLDER, "_processed_ids.txt")  # stocke des UID (stables)
ERROR_LOG_FILE  = os.path.join(DEST_FOLDER, "_error_ids.txt")

ME = "y.mandaba@gmail.com"
OTHERS = [
    "mymmandaba@gmail.com",
    "maryvonnemm@gmail.com",
    "maryvonne.masseron@free.fr",
    "maryvonnemm@hotmail.com",
    "maryvonnemm@hotmail.fr",
]

LIMIT_YEAR_FROM, LIMIT_YEAR_TO = 2000, 2030

# --- FAST MODE ---
USE_GMAIL_RAW   = True   # active X-GM-RAW (plus rapide chez GMail)
FETCH_TEXT_ONLY = True   # récupère seulement le texte (BODY.PEEK[TEXT])
BATCH_UIDS      = 200    # taille de lot pour FETCH groupé
LOG_EVERY       = 50     # log toutes les 50 entrées

# === UTILS ===
def clean_text_preserve_format(text: str) -> str:
    text = re.sub(r'<br\s*/?>', '\n', text or "", flags=re.I)
    text = re.sub(r'<[^>]+>', '', text or "")
    return text.strip()

def decode_mime_words(s):
    decoded = email.header.decode_header(s or "")
    return "".join([t.decode(c or "utf-8") if isinstance(t, bytes) else t for t,c in decoded])

def write_email(year, message_text):
    os.makedirs(DEST_FOLDER, exist_ok=True)
    path = os.path.join(DEST_FOLDER, f"emails_{year}.txt")
    with open(path, "a", encoding="utf-8") as f:
        f.write(message_text + "\n\n")
    return path

def load_processed_ids():
    if not os.path.exists(IDS_LOG_FILE): return set()
    with open(IDS_LOG_FILE, "r", encoding="utf-8") as f:
        return set(l.strip() for l in f if l.strip())

def save_processed_id(uid_str):
    os.makedirs(DEST_FOLDER, exist_ok=True)
    with open(IDS_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(uid_str + "\n")

def save_error_id(uid_str):
    os.makedirs(DEST_FOLDER, exist_ok=True)
    existing = set()
    if os.path.exists(ERROR_LOG_FILE):
        with open(ERROR_LOG_FILE, "r", encoding="utf-8") as f:
            existing = set(l.strip() for l in f)
    if uid_str not in existing:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(uid_str + "\n")

def clear_all_email_files():
    if not os.path.exists(DEST_FOLDER): return
    for fname in os.listdir(DEST_FOLDER):
        if fname.startswith("emails_") and fname.endswith(".txt"):
            os.remove(os.path.join(DEST_FOLDER, fname))
            print(f"🧹 Fichier supprimé : {fname}")
    for p in (IDS_LOG_FILE, ERROR_LOG_FILE):
        if os.path.exists(p):
            os.remove(p)
            print(f"🧹 {os.path.basename(p)} supprimé")

# --- Découverte dossiers Gmail ---
def _parse_list_line(line_bytes):
    """
    Parse une ligne de LIST en (flags:set[str], delim:str|None, name:str)
    Exemple brut:
      b'(\\HasNoChildren \\All) "/" "[Gmail]/All Mail"'
    """
    s = line_bytes.decode('utf-8', errors='ignore')
    # flags
    flags_part = ""
    if s.startswith("("):
        end = s.find(")")
        flags_part = s[1:end]
        s = s[end+1:].lstrip()
    flags = set(flags_part.split()) if flags_part else set()
    # delimiter
    delim = None
    if s.startswith('"'):
        q2 = s[1:].find('"')
        delim = s[1:1+q2]
        s = s[1+q2+1:].lstrip()
    # mailbox name
    name = s.strip()
    if name.startswith('"') and name.endswith('"'):
        name = name[1:-1]
    return flags, delim, name

def discover_all_mail_folder(mail):
    typ, data = mail.list()
    if typ != 'OK' or not data:
        return None
    candidates = []
    for line in data:
        if not line:
            continue
        flags, delim, name = _parse_list_line(line)
        if any(f.upper() == r'\ALL' for f in flags):
            candidates.append(name)
    if candidates:
        candidates.sort(key=lambda n: (0 if n.startswith('[Gmail]/') else 1, len(n)))
        return candidates[0]
    return None

def list_mailboxes_verbose(mail):
    typ, data = mail.list()
    if typ == 'OK' and data:
        print("📚 Dossiers IMAP disponibles :")
        for line in data:
            try:
                flags, delim, name = _parse_list_line(line)
                print(f"  - {name}   {flags}")
            except Exception:
                print(f"  - {line}")

# --- Connexion IMAP robuste + sélection dossier ---
def imap_connect():
    m = imaplib.IMAP4_SSL(IMAP_SERVER)
    m.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)

    list_mailboxes_verbose(m)
    auto_all = discover_all_mail_folder(m)

    folder_candidates = []
    if IMAP_FOLDER and IMAP_FOLDER.strip():
        folder_candidates.append(IMAP_FOLDER.strip())
    if auto_all and auto_all not in folder_candidates:
        folder_candidates.append(auto_all)
    if "INBOX" not in folder_candidates:
        folder_candidates.append("INBOX")

    last_err = None
    for cand in folder_candidates:
        try:
            typ, _ = m.select(f'"{cand}"')
            if typ == 'OK':
                print(f"📁 Dossier IMAP sélectionné: {cand}")
                return m
            else:
                last_err = f"SELECT {cand} -> {typ}"
        except Exception as e:
            last_err = f"{cand}: {e}"

    raise RuntimeError(f"Impossible d'ouvrir un dossier IMAP parmi {folder_candidates} — dernier essai: {last_err}")

# --- SAFE UID SEARCH/FETCH ---
def safe_uid_search(mail, query):
    for attempt in range(3):
        try:
            typ, data = mail.uid('search', None, query)
            return typ, data
        except imaplib.IMAP4.abort:
            time.sleep(1)
            mail = imap_connect()
    return "NO", [b""]

def safe_uid_fetch(mail, uid_bytes, text_only=True):
    cmd = '(BODY.PEEK[TEXT])' if text_only else '(RFC822)'
    for attempt in range(3):
        try:
            return mail.uid('fetch', uid_bytes, cmd)
        except imaplib.IMAP4.abort:
            time.sleep(1)
            mail = imap_connect()
    return "NO", None

# --- FAST SEARCH / BATCH FETCH ---
def build_gmail_raw_query(other, me, y_from, y_to):
    # Format YYYY/MM/DD pour GMail "after:" "before:"
    after = f"{y_from}/01/01"
    before = f"{y_to+1}/01/01"  # before est exclusif -> +1 an
    return f'((from:{other} to:{me}) OR (from:{me} to:{other})) after:{after} before:{before}'

def uid_search_fast(mail, other, me, y_from, y_to):
    if USE_GMAIL_RAW:
        raw = build_gmail_raw_query(other, me, y_from, y_to)
        typ, data = mail.uid('search', None, 'X-GM-RAW', raw)
        return typ, data
    else:
        typ1, msg1 = safe_uid_search(mail, f'(FROM "{other}" TO "{me}")')
        typ2, msg2 = safe_uid_search(mail, f'(FROM "{me}" TO "{other}")')
        if typ1 != "OK" and typ2 != "OK":
            return "NO", [b""]
        uids = set()
        if typ1 == "OK" and msg1 and msg1[0]: uids.update(msg1[0].split())
        if typ2 == "OK" and msg2 and msg2[0]: uids.update(msg2[0].split())
        return "OK", [b" ".join(sorted(uids, key=lambda x: int(x)))]

def uid_fetch_batch(mail, uid_list):
    """
    uid_list: list[bytes] d'UID
    Retourne dict{uid_str: raw_bytes}
    """
    if not uid_list:
        return {}
    seq = b",".join(uid_list)
    cmd = '(BODY.PEEK[TEXT])' if FETCH_TEXT_ONLY else '(RFC822)'
    typ, data = mail.uid('fetch', seq, cmd)
    if typ != 'OK' or not data:
        return {}
    out = {}
    uid_current = None
    for item in data:
        if not item:
            continue
        if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[0], (bytes, bytearray)):
            head = item[0].decode('utf-8', errors='ignore')
            m = re.match(r'\s*(\d+)\s+\(', head)
            if m:
                uid_current = m.group(1)
                out[uid_current] = item[1]
    return out

# --- MAIN EXPORT LOOP ---
def fetch_and_dump():
    os.makedirs(DEST_FOLDER, exist_ok=True)
    print(f"🗂 Dossier de sortie: {os.path.abspath(DEST_FOLDER)}")

    processed_ids = load_processed_ids()
    mail = imap_connect()

    for other in OTHERS:
        print(f"\n📨 Traitement de : {other}")

        typ, data = uid_search_fast(mail, other, ME, LIMIT_YEAR_FROM, LIMIT_YEAR_TO)
        if typ != "OK" or not data or not data[0]:
            print("ℹ️ Aucun message pour cette adresse avec le filtre actuel.")
            continue

        all_uids = [u for u in data[0].split() if u]
        all_uids = [u for u in all_uids if u.decode() not in processed_ids]

        print(f"🔢 {len(all_uids)} messages à récupérer (après filtre doublons).")
        if not all_uids:
            continue

        total = len(all_uids)
        for i in range(0, total, BATCH_UIDS):
            chunk = all_uids[i:i+BATCH_UIDS]

            fetched = uid_fetch_batch(mail, chunk)
            if not fetched:
                # fallback ultra-robuste: per-UID
                for u in chunk:
                    uid = u.decode()
                    try:
                        res, msg_data = safe_uid_fetch(mail, u, text_only=FETCH_TEXT_ONLY)
                        if res != "OK" or not msg_data or not msg_data[0]:
                            save_error_id(uid); continue
                        raw = msg_data[0][1]
                        handle_and_save_message(uid, raw)
                    except Exception:
                        save_error_id(uid)
                continue

            for j, u in enumerate(chunk, 1):
                uid = u.decode()
                raw = fetched.get(uid)
                if not raw:
                    save_error_id(uid); continue
                try:
                    handle_and_save_message(uid, raw)
                    idx = i + j
                    if idx % LOG_EVERY == 0 or idx == total:
                        print(f"✅ {idx}/{total} · {other} · UID {uid}")
                except Exception:
                    save_error_id(uid)

    print("✅ Export terminé.")
    try:
        mail.logout()
    except Exception:
        pass

def handle_and_save_message(uid, raw_bytes):
    msg = email.message_from_bytes(raw_bytes)

    from_    = decode_mime_words(msg.get("From"))
    to       = decode_mime_words(msg.get("To"))
    subject  = decode_mime_words(msg.get("Subject") or "(sans sujet)")
    date_str = msg.get("Date", "")

    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        year = dt.year; date_fmt = dt.strftime("%Y-%m-%d")
    except Exception:
        year = "inconnu"; date_fmt = date_str

    if isinstance(year, int) and not (LIMIT_YEAR_FROM <= year <= LIMIT_YEAR_TO):
        save_processed_id(uid)
        return

    # Corps (si FETCH_TEXT_ONLY, on a généralement le texte directement)
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if "attachment" in str(part.get("Content-Disposition") or ""):
                continue
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True) or b""
                try:
                    body = payload.decode(part.get_content_charset() or "utf-8", errors="ignore")
                except Exception:
                    body = payload.decode(errors="ignore")
                break
    else:
        payload = msg.get_payload(decode=True) or b""
        try:
            body = payload.decode(msg.get_content_charset() or "utf-8", errors="ignore")
        except Exception:
            body = payload.decode(errors="ignore")

    body = clean_text_preserve_format(body)

    output = f"""=== MESSAGE ===
🗕 Date : {date_fmt}
👤 From : {from_}
📨 To   : {to}
🧕 Subject : {subject}
---
// UID: {uid}
{body}
=== FIN ===""".strip()

    out_path = write_email(year, output)
    save_processed_id(uid)

if __name__ == "__main__":
    if "--reset" in sys.argv or "-r" in sys.argv:
        print("🔁 Réinitialisation demandée…")
        clear_all_email_files()
    else:
        fetch_and_dump()
