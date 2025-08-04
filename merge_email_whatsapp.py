# feature : Multiline

import os
import re
from datetime import datetime

ORIGIN_FOLDER = "Formated_msg-to-merge"
TARGET_FOLDER = "txt"
LIMIT_YEAR_FROM = 2000
LIMIT_YEAR_TO = 2030

# Pattern pour détecter les blocs standardisés
MESSAGE_BLOCK = re.compile(r"=== MESSAGE ===\n🗕 Date : (.*?)\n👤 From : (.*?)\n📨 To   : (.*?)\n🧕 Subject : (.*?)\n---\n(.*?)\n=== FIN ===", re.DOTALL)

def extract_messages_from_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    messages = []
    for match in MESSAGE_BLOCK.finditer(content):
        try:
            date_str, author, to, subject, body = match.groups()
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            messages.append({
                "date": date_obj,
                "author": author,
                "to": to,
                "subject": subject,
                "body": body.strip()
            })
        except Exception:
            continue
    return messages

def merge_all_messages():
    os.makedirs(TARGET_FOLDER, exist_ok=True)
    all_messages = []
    rejected_files = 0

    for filename in os.listdir(ORIGIN_FOLDER):
        path = os.path.join(ORIGIN_FOLDER, filename)
        if not os.path.isfile(path):
            continue

        try:
            msgs = extract_messages_from_file(path)
            if not msgs:
                rejected_files += 1
                continue
            all_messages.extend(msgs)
        except Exception:
            rejected_files += 1
            continue

    # Trie par date
    all_messages.sort(key=lambda x: x["date"])

    # Écriture groupée par année
    count_written = 0
    for msg in all_messages:
        year = msg["date"].year
        if not (LIMIT_YEAR_FROM <= year <= LIMIT_YEAR_TO):
            continue

        date_fmt = msg["date"].strftime("%Y-%m-%d")
        output = f"""
=== MESSAGE ===
🗕 Date : {date_fmt}
👤 From : {msg['author']}
📨 To   : {msg['to']}
🧕 Subject : {msg['subject']}
---
{msg['body']}
=== FIN ===
""".strip()

        target_file = os.path.join(TARGET_FOLDER, f"emails_{year}.txt")
        if os.path.exists(target_file):
            with open(target_file, "r", encoding="utf-8") as f:
                if output in f.read():
                    continue

        with open(target_file, "a", encoding="utf-8") as f:
            f.write(output + "\n\n")
            count_written += 1

    print(f"✅ Fusion terminée. {count_written} messages ajoutés.")
    print(f"❌ {rejected_files} fichiers rejetés (format invalide).")

if __name__ == "__main__":
    merge_all_messages()
