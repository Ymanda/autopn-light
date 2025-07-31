import os
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import html2text
from dotenv import load_dotenv
import openai
from datetime import datetime

load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
PERSON_EMAIL = os.getenv("PERSON_EMAIL")
CONTEXT_FILES = [f.strip() for f in os.getenv("CONTEXT_FILES", "").split(",") if f.strip()]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

ARCHIVE_DIR = "archives_emails"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not PERSON_EMAIL or not OPENAI_API_KEY:
    raise ValueError("❌ Variables manquantes dans le .env")

openai.api_key = OPENAI_API_KEY

def connect_gmail():
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        mail.select("inbox")
        return mail
    except Exception as e:
        print("❌ Erreur de connexion :", e)
        return None

def clean_subject(subject):
    if subject is None:
        return "(Sans objet)"
    decoded, charset = decode_header(subject)[0]
    return decoded.decode(charset or "utf-8", errors="replace") if isinstance(decoded, bytes) else decoded

def extract_body(msg):
    for part in msg.walk():
        content_type = part.get_content_type()
        content_dispo = str(part.get("Content-Disposition"))
        if "attachment" in content_dispo:
            continue
        try:
            charset = part.get_content_charset() or "utf-8"
            if content_type == "text/plain":
                return part.get_payload(decode=True).decode(charset, errors="replace")
            elif content_type == "text/html":
                html = part.get_payload(decode=True).decode(charset, errors="replace")
                return html2text.html2text(html)
        except Exception as e:
            continue
    return "(Pas de contenu lisible)"

def download_emails():
    mail = connect_gmail()
    if not mail:
        return

    result, data = mail.search(None, f'(OR FROM "{PERSON_EMAIL}" TO "{PERSON_EMAIL}")')
    if result != 'OK':
        print("❌ Échec de la recherche IMAP")
        return

    message_ids = data[0].split()
    emails_by_year = {}

    for num in message_ids:
        result, msg_data = mail.fetch(num, '(RFC822)')
        if result != 'OK':
            continue

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)
        sender = email.utils.parseaddr(msg.get("From"))[1]
        subject = clean_subject(msg.get("Subject"))
        date_obj = parsedate_to_datetime(msg.get("Date"))
        year = date_obj.year
        body = extract_body(msg)

        entry = (
            date_obj,
            f"=== [{date_obj}] De: {sender} ===\nObjet : {subject}\n\n{body.strip()}\n\n"
        )
        emails_by_year.setdefault(year, []).append(entry)

    for year, entries in emails_by_year.items():
        entries.sort(key=lambda x: x[0])
        with open(f"{ARCHIVE_DIR}/echange_{year}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join([entry for _, entry in entries]))

def build_context(max_words=6000):
    total_words = 0
    lines = []

    for filename in CONTEXT_FILES:
        filepath = os.path.join(ARCHIVE_DIR, filename)
        if not os.path.exists(filepath):
            print(f"⚠️ Fichier introuvable : {filepath}")
            continue

        with open(filepath, encoding="utf-8") as f:
            for line in f:
                word_count = len(line.split())
                if total_words + word_count > max_words:
                    break
                lines.append(line)
                total_words += word_count

        if total_words >= max_words:
            break

    with open("contexte_actuel.txt", "w", encoding="utf-8") as out:
        out.writelines(lines)

    print(f"✅ Contexte généré : {total_words} mots dans 'contexte_actuel.txt'")

def generer_reponse(nouveau_message: str):
    with open("contexte_actuel.txt", encoding="utf-8") as f:
        contexte = f.read()

    prompt_systeme = """
Tu es un assistant personnel chargé de répondre automatiquement à une personne qui envoie des e-mails manipulateurs.
Tu agis pour Yannick, de façon calme, rationnelle, structurée, en t'appuyant sur les faits.
Ignore les provocations, clarifie les points importants, et ferme la discussion quand c’est pertinent.
"""

    messages = [
        {"role": "system", "content": prompt_systeme},
        {"role": "user", "content": f"Voici le contexte :\n{contexte}"},
        {"role": "user", "content": f"Voici le nouveau message reçu :\n{nouveau_message}\n\nQuelle réponse Yannick doit-il envoyer ?"}
    ]

    print("⏳ Génération de la réponse...")
    completion = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.4
    )

    reponse = completion.choices[0].message.content
    print("\n✅ Réponse générée :\n")
    print(reponse)

# --- Utilisation ---
if __name__ == "__main__":
    print("1) 📥 Archivage des e-mails...")
    download_emails()

    print("2) 🧠 Construction du contexte (max 6000 mots)...")
    build_context()

    print("3) 📨 Colle ci-dessous le message reçu :")
    nouveau = input("\nMessage :\n")
    generer_reponse(nouveau)
