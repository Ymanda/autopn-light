#Script mis à jour :
#Affiche clairement l’adresse en cours de traitement.
#Compteur par e-mail (avec total et ID).
#Message si aucun message trouvé pour une adresse.
#Affiche les erreurs si un e-mail pose problème (sans planter).
#Ne duplique pas les messages existants.
  
  
import imaplib
import email
from email.header import decode_header
from datetime import datetime
import os
import re
import sys
import time

# === CONFIGURATION ===
IMAP_SERVER = "imap.gmail.com"
EMAIL_ACCOUNT = "y.mandaba@gmail.com"
EMAIL_PASSWORD = ""
DEST_FOLDER = "txt_y.mandaba"
ME = "y.mandaba@gmail.com"
OTHERS = [  # Liste d'adresses
    "mymmandaba@gmail.com",
    "maryvonnemm@gmail.com",
    "maryvonne.masseron@free.fr",
    "maryvonnemm@hotmail.com",
    "maryvonnemm@hotmail.fr"
    # Ajouter d'autres e-mails ici si besoin
]
PAUSE_BETWEEN_MSGS = 0.1  # secondes (peut être réduit à 0.01 si stable)
LIMIT_YEAR_FROM = 2000
LIMIT_YEAR_TO = 2030

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
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            if message_text in f.read():
                return
    with open(filename, "a", encoding="utf-8") as f:
        f.write(message_text + "\n\n")

# === MAIN ===
def fetch_and_dump():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
    mail.select("inbox")

    for other in OTHERS:
        print(f"\n📨 Traitement de : {other}")
        status1, msg1 = mail.search(None, f'(FROM "{other}" TO "{ME}")')
        status2, msg2 = mail.search(None, f'(FROM "{ME}" TO "{other}")')

        if status1 != "OK" and status2 != "OK":
            print(f"⚠️ Aucun message trouvé avec {other}")
            continue

        ids = set()
        if status1 == "OK":
            ids.update(msg1[0].split())
        if status2 == "OK":
            ids.update(msg2[0].split())

        print(f"🔢 {len(ids)} messages trouvés avec {other}.")

        for i, num in enumerate(sorted(ids), 1):
            try:
                print(f"⏳ [{i}/{len(ids)}] ID {num.decode()}...")
                res, msg_data = mail.fetch(num, "(RFC822)")
                if res != "OK":
                    print(f"⚠️ Erreur fetch pour ID {num.decode()}")
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

                if isinstance(year, int) and (year < LIMIT_YEAR_FROM or year > LIMIT_YEAR_TO):
                    print(f"⏩ Message ignoré (année {year} hors limite)")
                    continue

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

                print(f"🗂 Enregistrement dans : emails_{year}.txt")
                write_email(year, output)

                time.sleep(PAUSE_BETWEEN_MSGS)
                sys.stdout.flush()

            except Exception as e:
                print(f"❌ Erreur sur le message {num.decode()}: {e}")

    print("✅ Export terminé.")
    mail.logout()

if __name__ == "__main__":
    fetch_and_dump()
