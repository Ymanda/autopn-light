import os
import csv
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from imapclient import IMAPClient
import pyzmail
import openai
import smtplib
from email.mime.text import MIMEText

# === CONFIG ===
CSV_FILE = "paiements.csv"
TRACKER_FILE = ".rappel_tracker.json"
RAPPEL_INTERVAL_HEURES = 48
DUE_INCREMENT = 500
GPT_ENABLED = True

load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
PERSON_EMAIL = os.getenv("PERSON_EMAIL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT"))

TODAY = datetime.today().date()
openai.api_key = OPENAI_API_KEY

# === CSV ===
def read_csv():
    if not os.path.exists(CSV_FILE):
        return []
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_csv(rows):
    with open(CSV_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["date", "paiement", "due", "note"])
        writer.writeheader()
        writer.writerows(rows)

def generate_due_entries(rows):
    if not rows:
        last_date = datetime(TODAY.year, TODAY.month, 1).date()
    else:
        last_date = datetime.strptime(rows[-1]["date"], "%Y-%m-%d").date()

    while last_date < TODAY:
        next_date = get_next_due_date(last_date)
        if next_date > TODAY:
            break
        rows.append({
            "date": next_date.strftime("%Y-%m-%d"),
            "paiement": "0",
            "due": str(DUE_INCREMENT),
            "note": f"compensation automatique du {next_date.day}"
        })
        last_date = next_date
    return rows

def get_next_due_date(d):
    if d.day < 15:
        return d.replace(day=15)
    else:
        first_of_next_month = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
        return first_of_next_month

# === TZCOMP ===
def scan_for_tzcomp(rows):
    with IMAPClient("imap.gmail.com") as server:
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        server.select_folder("INBOX")
        messages = server.search(['FROM', PERSON_EMAIL])
        responses = server.fetch(messages, ['ENVELOPE', 'RFC822'])

        for msgid, data in responses.items():
            subject = data[b'ENVELOPE'].subject.decode(errors='ignore')
            if subject.lower().startswith("tzcomp="):
                montant = float(subject[7:].strip())
                already_recorded = any(
                    float(r["paiement"]) == montant and r["date"] == TODAY.strftime("%Y-%m-%d")
                    for r in rows
                )
                if not already_recorded:
                    rows.append({
                        "date": TODAY.strftime("%Y-%m-%d"),
                        "paiement": str(montant),
                        "due": "0",
                        "note": f"tzcomp={montant}"
                    })
                    print(f"📩 Paiement de {montant}€ ajouté depuis e-mail tzcomp=")
    return rows

# === GPT + EMAIL ===
def load_tracker():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_tracker(data):
    with open(TRACKER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)

def generate_gpt_reminder(entry):
    contexte = f"""
Yannick attend un paiement de {entry['due']} € depuis le {entry['date']}.
La personne n'a pas payé. Elle est coutumière des retards.
Rédige un rappel ferme, factuel, sans agressivité.
"""
    prompt = [{"role": "system", "content": contexte}]
    prompt.append({"role": "user", "content": "Rédige le message à envoyer."})
    res = openai.ChatCompletion.create(model="gpt-4o", messages=prompt, temperature=0.3)
    return res.choices[0].message.content.strip()

def extract_last_paiements(rows, count=3):
    paiements = [r for r in rows if float(r["paiement"]) > 0]
    return paiements[-count:]

def send_email(to, sujet, message_text, recent_rows=None):
    # Résumé solde + derniers paiements
    total_due = sum(float(r["due"]) for r in recent_rows)
    total_paye = sum(float(r["paiement"]) for r in recent_rows)
    solde = max(0, total_due - total_paye)

    message_text += f"\n\n--- RÉSUMÉ ---\n"
    message_text += f"Montant total dû : {total_due:.2f} €\n"
    message_text += f"Montant total payé : {total_paye:.2f} €\n"
    message_text += f"Solde restant : {solde:.2f} €\n"

    derniers = extract_last_paiements(recent_rows)
    if derniers:
        message_text += "\nDerniers paiements :\n"
        for r in derniers:
            message_text += f"- {r['date']} : {r['paiement']} €"
            if r.get("note"):
                message_text += f" ({r['note']})"
            message_text += "\n"

    msg = MIMEText(message_text, "plain", "utf-8")
    msg["Subject"] = sujet
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = to

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, [to], msg.as_string())
        print(f"📤 Rappel envoyé à {to}")
    except Exception as e:
        print(f"❌ Erreur SMTP : {e}")

# === RAPPELS ===
def process_overdue_reminders(rows):
    tracker = load_tracker()
    for row in rows:
        date_str = row["date"]
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        if date_obj >= TODAY:
            continue
        paiement = float(row["paiement"])
        if paiement >= float(row["due"]):
            continue

        last_reminder = tracker.get(date_str)
        if last_reminder:
            delta = (datetime.now() - datetime.fromisoformat(last_reminder)).total_seconds() / 3600
            if delta < RAPPEL_INTERVAL_HEURES:
                continue

        print(f"🔔 Rappel dû pour le {date_str} ({paiement} € payés)")

        if GPT_ENABLED:
            msg = generate_gpt_reminder(row)
        else:
            msg = f"Bonjour,\n\nNous n'avons pas reçu le paiement prévu pour le {date_str}.\nMerci de régulariser rapidement."

        send_email(PERSON_EMAIL, f"Rappel de paiement du {date_str}", msg, recent_rows=rows)
        tracker[date_str] = datetime.now().isoformat()
        save_tracker(tracker)

# === RÉSUMÉ TERMINAL ===
def show_summary(rows):
    total_due = sum(float(r["due"]) for r in rows)
    total_paid = sum(float(r["paiement"]) for r in rows)

    print(f"\n📊 Résumé paiements :")
    print(f"Total payé     : {total_paid:.2f} €")
    print(f"Montant dû     : {total_due:.2f} €")
    print(f"Solde restant  : {max(0, total_due - total_paid):.2f} €")

# === MAIN ===
if __name__ == "__main__":
    rows = read_csv()
    rows = generate_due_entries(rows)
    rows = scan_for_tzcomp(rows)
    process_overdue_reminders(rows)
    write_csv(rows)
    show_summary(rows)
