import csv
import os
from datetime import datetime

FILENAME = "paiements.csv"
DATE_FORMAT = "%Y-%m-%d"

def read_payments():
    if not os.path.exists(FILENAME):
        print(f"❌ Fichier {FILENAME} introuvable.")
        return []
    with open(FILENAME, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def format_row(row, col_widths):
    return " | ".join([
        row["date"].ljust(col_widths[0]),
        row["paiement"].rjust(col_widths[1]),
        row["due"].rjust(col_widths[2]),
        row.get("note", "").ljust(col_widths[3])
    ])

def show_table(rows):
    headers = ["Date", "Paiement (€)", "Dû (€)", "Note"]
    col_widths = [10, 12, 8, 30]

    print("\n" + "=" * (sum(col_widths) + 9))
    print(" | ".join([
        headers[0].ljust(col_widths[0]),
        headers[1].center(col_widths[1]),
        headers[2].center(col_widths[2]),
        headers[3].ljust(col_widths[3])
    ]))
    print("-" * (sum(col_widths) + 9))

    for row in rows[-10:]:
        print(format_row(row, col_widths))

    print("=" * (sum(col_widths) + 9))

def show_summary(rows):
    total_due = 0
    total_paid = 0

    for row in rows:
        total_due += float(row["due"])
        total_paid += float(row["paiement"])

    print(f"\nTotal payé     : {total_paid:.2f} €")
    print(f"Montant dû     : {total_due:.2f} €")
    print(f"Solde restant  : {max(0, total_due - total_paid):.2f} €")


if __name__ == "__main__":
    rows = read_payments()
    if not rows:
        exit()

    show_table(rows)
    show_summary(rows)
