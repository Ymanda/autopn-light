import os
from dotenv import load_dotenv
from imapclient import IMAPClient
import pyzmail
from datetime import datetime

load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
ARCHIVE_DIR = "archives_emails"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def fetch_and_archive_emails(person_email: str, since_date_str: str):
    since_date = datetime.strptime(since_date_str, "%Y-%m-%d")
    with IMAPClient("imap.gmail.com") as server:
        print("📥 Connexion à Gmail...")
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.select_folder("INBOX")

        criteria = [
            'AND',
            ['SINCE', since_date.strftime('%d-%b-%Y')],
            ['OR', ['FROM', person_email], ['TO', person_email]]
        ]

        print("🔍 Recherche des e-mails...")
        messages = server.search(criteria)
        responses = server.fetch(messages, ['ENVELOPE', 'RFC822'])

        emails_by_year = {}

        for msgid, data in responses.items():
            raw_message = pyzmail.PyzMessage.factory(data[b'RFC822'])
            date = data[b'ENVELOPE'].date
            year = date.year
            subject = raw_message.get_subject()
            sender = raw_message.get_addresses('from')[0][1]

            if raw_message.text_part:
                body = raw_message.text_part.get_payload().decode(raw_message.text_part.charset, errors='replace')
            elif raw_message.html_part:
                body = "(HTML uniquement - ignoré)"
            else:
                body = "(Pas de contenu lisible)"

            entry = f"=== [{date.strftime('%Y-%m-%d %H:%M')}] De: {sender} ===\n"
            entry += f"Objet : {subject or '(Sans objet)'}\n\n{body.strip()}\n\n"

            emails_by_year.setdefault(year, []).append(entry)

        for year, messages in emails_by_year.items():
            filename = f"{ARCHIVE_DIR}/echange_{year}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write("\n".join(sorted(messages)))
            print(f"✅ Fichier sauvegardé : {filename}")

        print("\n✅ Archivage terminé.")

if __name__ == "__main__":
    person_email = input("Adresse e-mail cible ? > ").strip()
    since_date = input("Depuis quelle date ? (YYYY-MM-DD) > ").strip()
    fetch_and_archive_emails(person_email, since_date)
