import imaplib
import email
from email.header import decode_header
from datetime import datetime
import os
import re

# === CONFIGURATION ===
IMAP_SERVER = "imap.gmail.com"
EMAIL_ACCOUNT = "ymanda@gmail.com"
EMAIL_PASSWORD = "nsro bcnp djmn euys"
DEST_FOLDER = "txt_ymanda"
ME = "ymanda@gmail.com"
OTHER = "mymmandaba@gmail.com"

# === UTILS ===
def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text)  # remove HTML tags
    text = re.sub(r'\s+', ' ', text)  # collapse spaces
    return text.strip()

def decode_mime_words(s):
    decoded = email.header.decode_header(s)
    return ''.join([
        t.decode(c or 'utf-8') if isinstance(t, bytes) else t
        for t, c in decoded
    ])

def write_email(year, message_text):
    os.makedirs(DEST_FOLDER, exist_ok=True)
    filename = os.path.join(DEST_FOLDER, f"emails_{year}.txt")
    with open(filename, "a", encoding="utf-8") as f:
        f.write(message_text + "\n\n")

# === MAIN ===
def fetch_and_dump():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
    mail.select("inbox")

    # Reçu de la mère → moi
    status1, msg1 = mail.search(None, f'(FROM "{OTHER}" TO "{ME}")')
    # Envoyé par moi → mère
    status2, msg2 = mail.search(None, f'(FROM "{ME}" TO "{OTHER}")')

    if status1 != "OK" or status2 != "OK":
        print("Erreur lors de la recherche.")
        return

    all_ids = set(msg1[0].split()) | set(msg2[0].split())

    for num in sorted(all_ids):
        res, msg_data = mail.fetch(num, "(RFC822)")
        if res != "OK":
            continue

        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)

        from_ = decode_mime_words(msg.get("From", ""))
        to = decode_mime_words(msg.get("To", ""))
        subject = decode_mime_words(msg.get("Subject", "(sans sujet)"))
        date_str = msg.get("Date", "")
        try:
            date_obj = email.utils.parsedate_to_datetime(date_str)
            year = date_obj.year
            date_fmt = date_obj.strftime("%Y-%m-%d")
        except:
            year = "inconnu"
            date_fmt = date_str

        # Récupérer le corps
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))
                if "attachment" in content_disposition:
                    continue
                if content_type == "text/plain":
                    body = part.get_payload(decode=True).decode(errors="ignore")
                    break
        else:
            body = msg.get_payload(decode=True).decode(errors="ignore")

        body = clean_text(body)

        output = f"""
=== MESSAGE ===
🗕 Date : {date_fmt}
👤 From : {from_}
📨 To   : {to}
🧕 Subject : {subject}
---
{body}
=== FIN ===
""".strip()

        write_email(year, output)

    print("✅ Export terminé.")

if __name__ == "__main__":
    fetch_and_dump()
