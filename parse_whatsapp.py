"""Convert a WhatsApp export into the AutoPN plain-text email format."""

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from autopn.config import (
    expand_path,
    load_config,
    owner_label,
    resolve_relation,
)

WHATSAPP_LINE = re.compile(
    r"^(\d{1,2}/\d{1,2}/\d{2,4}), (\d{1,2}:\d{2})[\u202f ]?(AM|PM|am|pm)? - ([^:]+): (.*)$"
)


def parse_datetime(date_str: str, time_str: str, ampm: Optional[str]) -> Optional[datetime]:
    """Handle both 12h and 24h WhatsApp exports."""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            if ampm:
                return datetime.strptime(
                    f"{date_str} {time_str} {ampm.upper()}",
                    f"{fmt} %I:%M %p",
                )
            return datetime.strptime(f"{date_str} {time_str}", f"{fmt} %H:%M")
        except ValueError:
            continue
    return None


def read_lines(path: Path) -> List[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1").splitlines()


def build_output_block(author: str, recipient: str, subject: str, date_obj: datetime, message: str) -> str:
    date_fmt = date_obj.strftime("%Y-%m-%d")
    return (
        "=== MESSAGE ===\n"
        f"🗕 Date : {date_fmt}\n"
        f"👤 From : {author.strip()}\n"
        f"📨 To   : {recipient}\n"
        f"🧕 Subject : {subject}\n"
        "---\n"
        f"{message.strip()}\n"
        "=== FIN ==="
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="Path to config/autopn.yaml (defaults to env/AUTOPN_CONFIG or repo copy).")
    parser.add_argument("--relation", help="Relation id defined in the config (defaults to the sole relation).")
    parser.add_argument("--chat-file", help="Override WhatsApp export file path.")
    parser.add_argument("--output-dir", help="Override destination folder for formatted txt files.")
    parser.add_argument("--subject", help="Override subject used in the formatted blocks.")
    parser.add_argument("--recipient", help="Override recipient label (defaults to owner.name).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    relation = resolve_relation(config, args.relation)

    whatsapp_cfg = relation.get("whatsapp") or {}
    chat_file = args.chat_file or whatsapp_cfg.get("chat_export")
    if not chat_file:
        raise ValueError("Missing WhatsApp chat_export in config.")
    chat_path = expand_path(chat_file)
    if not chat_path.exists():
        raise FileNotFoundError(f"WhatsApp export not found: {chat_path}")

    output_dir = args.output_dir or whatsapp_cfg.get("email_output_dir")
    if not output_dir:
        output_dir = chat_path.parent / "emailized"
    out_path = expand_path(str(output_dir))
    out_path.mkdir(parents=True, exist_ok=True)

    subject = args.subject or whatsapp_cfg.get("subject") or "WhatsApp"
    recipient = args.recipient or whatsapp_cfg.get("recipient_label") or owner_label(config)

    year_cfg = (whatsapp_cfg.get("year_range") or {})
    year_from = int(year_cfg.get("from", 2000))
    year_to = int(year_cfg.get("to", 2100))

    lines = read_lines(chat_path)
    messages = []
    current = None
    for line in lines:
        match = WHATSAPP_LINE.match(line)
        if match:
            if current:
                messages.append(current)
            date_str, time_str, ampm, author, body = match.groups()
            dt = parse_datetime(date_str, time_str, ampm)
            if not dt or not (year_from <= dt.year <= year_to):
                current = None
                continue
            current = {"date": dt, "author": author.strip(), "message": body.strip()}
        elif current:
            current["message"] += "\n" + line.strip()
    if current:
        messages.append(current)

    dedup_cache = {}
    for msg in messages:
        block = build_output_block(msg["author"], recipient, subject, msg["date"], msg["message"])
        year = msg["date"].year
        target_file = out_path / f"whatsapp_{year}.txt"
        dedup_cache.setdefault(target_file, target_file.read_text(encoding="utf-8") if target_file.exists() else "")
        if block in dedup_cache[target_file]:
            continue
        with target_file.open("a", encoding="utf-8") as handle:
            handle.write(block + "\n\n")
        dedup_cache[target_file] += block

    print(f"✅ {len(messages)} messages processed for relation '{relation.get('id')}'.")


if __name__ == "__main__":
    main()
