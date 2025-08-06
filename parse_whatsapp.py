# feature : Multiline + fallback encodage sans chardet
import os
import re
from datetime import datetime

WHATSAPP_FILE = "relations/Bee/WhatsApp Chat with Bee Baby"
DEST_FOLDER = "relations/Bee/txt_whatsapp"
LIMIT_YEAR_FROM = 2000
LIMIT_YEAR_TO = 2030

WHATSAPP_LINE = re.compile(
    r"^(\d{1,2}/\d{1,2}/\d{2,4}), (\d{1,2}:\d{2})\s*[\u202f ]?(AM|PM|am|pm)? - ([^:]+): (.*)$"
)

def parse_datetime(date_str, time_str, ampm):
    for fmt in ["%m/%d/%y", "%m/%d/%Y"]:
        try:
            if ampm:
                dt = datetime.strptime(f"{date_str} {time_str} {ampm.upper()}", f"{fmt} %I:%M %p")
            else:
                dt = datetime.strptime(f"{date_str} {time_str}", f"{fmt} %H:%M")
            return dt
        except Exception:
            continue
    return None

def read_lines_fallback(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.readlines()
    except UnicodeDecodeError:
        print("⚠️ Erreur UTF-8, tentative en latin1...")
        with open(filepath, "r", encoding="latin1") as f:
            return f.readlines()

def parse_whatsapp():
    if not os.path.exists(WHATSAPP_FILE):
        print(f"❌ Fichier introuvable : {WHATSAPP_FILE}")
        return

    os.makedirs(DEST_FOLDER, exist_ok=True)

    lines = read_lines_fallback(WHATSAPP_FILE)
    messages = []
    current = None

    for line in lines:
        match = WHATSAPP_LINE.match(line)
        if match:
            if current:
                messages.append(current)
            date_str, time_str, ampm, author, message = match.groups()
            dt = parse_datetime(date_str, time_str, ampm)
            if not dt or not (LIMIT_YEAR_FROM <= dt.year <= LIMIT_YEAR_TO):
                current = None
                continue
            current = {
                "date": dt,
                "author": author.strip(),
                "message": message.strip()
            }
        else:
            if current:
                current["message"] += "\n" + line.strip()

    if current:
        messages.append(current)

    for msg in messages:
        date_fmt = msg["date"].strftime("%Y-%m-%d")
        output = f"""
=== MESSAGE ===
🗕 Date : {date_fmt}
👤 From : {msg['author']}
📨 To   : Yannick
🧕 Subject : WhatsApp
---
{msg['message']}
=== FIN ===
""".strip()

        year = msg["date"].year
        filename = os.path.join(DEST_FOLDER, f"whatsapp_{year}.txt")
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                if output in f.read():
                    continue

        with open(filename, "a", encoding="utf-8") as f:
            f.write(output + "\n\n")

    print("✅ WhatsApp intégré avec succès.")

if __name__ == "__main__":
    parse_whatsapp()
