import os
import re
from datetime import datetime

# === CONFIGURATION ===
WHATSAPP_FILE = "whatsapp_export.txt"
DEST_FOLDER = "txt"
LIMIT_YEAR_FROM = 2000
LIMIT_YEAR_TO = 2030

# Format WhatsApp : "31/12/23, 21:45 - Marc: Salut !"
WHATSAPP_LINE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{2,4}), (\d{1,2}:\d{2}) - ([^:]+): (.*)$")


def parse_datetime(date_str, time_str):
    for fmt in ["%d/%m/%y", "%d/%m/%Y"]:
        try:
            date = datetime.strptime(date_str, fmt)
            break
        except:
            continue
    else:
        return None

    dt = datetime.strptime(f"{date_str} {time_str}", f"{fmt} %H:%M")
    return dt


def parse_whatsapp():
    if not os.path.exists(WHATSAPP_FILE):
        print(f"❌ Fichier introuvable : {WHATSAPP_FILE}")
        return

    os.makedirs(DEST_FOLDER, exist_ok=True)

    with open(WHATSAPP_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        match = WHATSAPP_LINE.match(line)
        if not match:
            continue

        date_str, time_str, author, message = match.groups()
        dt = parse_datetime(date_str, time_str)
        if not dt:
            continue

        year = dt.year
        if not (LIMIT_YEAR_FROM <= year <= LIMIT_YEAR_TO):
            continue

        date_fmt = dt.strftime("%Y-%m-%d")
        output = f"""
=== MESSAGE ===
🗕 Date : {date_fmt}
👤 From : {author}
📨 To   : Yannick
🧕 Subject : WhatsApp
---
{message}
=== FIN ===
""".strip()

        filename = os.path.join(DEST_FOLDER, f"emails_{year}.txt")
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                if output in f.read():
                    continue

        with open(filename, "a", encoding="utf-8") as f:
            f.write(output + "\n\n")

    print("✅ WhatsApp intégré avec succès.")


if __name__ == "__main__":
    parse_whatsapp()
