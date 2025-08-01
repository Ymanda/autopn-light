import os
from dotenv import load_dotenv
from imapclient import IMAPClient
import pyzmail
import openai
from datetime import datetime

load_dotenv()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
PERSON_EMAIL = os.getenv("PERSON_EMAIL")
CONTEXT_FILES = os.getenv("CONTEXT_FILES", "").split(",")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

ARCHIVE_DIR = "archives_emails"
openai.api_key = OPENAI_API_KEY

def build_context(max_words=6000):
    total_words = 0
    lines = []

    for filename in CONTEXT_FILES:
        filepath = os.path.join(ARCHIVE_DIR, filename.strip())
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

def fetch_last_email_from(person_email):
    with IMAPClient("imap.gmail.com") as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.select_folder("INBOX")

        criteria = ['FROM', person_email]
        messages = server.search(criteria)
        if not messages:
            print("❌ Aucun message trouvé.")
            return None

        last_id = sorted(messages)[-1]
        response = server.fetch([last_id], ['ENVELOPE', 'RFC822'])
        raw_message = pyzmail.PyzMessage.factory(response[last_id][b'RFC822'])

        date = response[last_id][b'ENVELOPE'].date
        subject = raw_message.get_subject()
        sender = raw_message.get_addresses('from')[0][1]

        if raw_message.text_part:
            body = raw_message.text_part.get_payload().decode(raw_message.text_part.charset, errors='replace')
        elif raw_message.html_part:
            body = "(HTML uniquement - ignoré)"
        else:
            body = "(Aucun contenu)"

        return {
            "date": date,
            "subject": subject,
            "from": sender,
            "body": body.strip()
        }

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

    print("⏳ Génération de la réponse en cours...")
    completion = openai.ChatCompletion.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.5
    )

    reponse = completion.choices[0].message.content
    print("\n✅ Réponse générée :\n")
    print(reponse)

# --- Utilisation ---
if __name__ == "__main__":
    print("1) Construction du contexte (max 6000 mots)...")
    build_context()

    print("2) Récupération du dernier message...")
    email_data = fetch_last_email_from(PERSON_EMAIL)

    if email_data:
        nouveau = f"""Message reçu le {email_data['date']} de {email_data['from']} :
Objet : {email_data['subject']}

{email_data['body']}
"""
        generer_reponse(nouveau)
