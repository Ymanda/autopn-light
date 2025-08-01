import os
import openai
from datetime import datetime

# === Configuration ===
YEAR = "2024"
INPUT_FILE = f"txt/emails_{YEAR}.txt"
OUTPUT_FILE = f"rapport/{YEAR}_sophismes.md"
MODEL = "gpt-4o"
CHUNK_SIZE = 6000  # characters

# === GPT prompt template ===
PROMPT_TEMPLATE = """
Tu es un analyste spécialisé en communication manipulatoire.
Voici un extrait d'e-mails entre Yannick et sa mère pour l'année {year}.

Ta tâche :
- Identifier les sophismes utilisés par sa mère (pas ceux de Yannick)
- Classer chaque sophisme (catégorie, nom)
- Expliquer pourquoi il s'agit d'un sophisme
- Résumer les points importants abordés dans cet extrait (terrains, entreprises, conflits familiaux)

Format attendu :
### Analyse Bloc X
- 📅 Dates couvertes : ...
- 🧠 Sophismes détectés :
  - "Citation" → [Nom] — [Catégorie] — [Explication]
- 🧩 Points majeurs abordés :
  - ...
"""

# === Fonction utilitaire ===
def read_chunks(file_path, max_chars):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    chunks = []
    while content:
        chunk = content[:max_chars]
        last_cut = chunk.rfind("\n\n")
        if last_cut == -1:
            last_cut = len(chunk)
        chunks.append(content[:last_cut].strip())
        content = content[last_cut:].strip()
    return chunks

# === Appel OpenAI ===
def analyze_chunk(chunk, block_id):
    prompt = PROMPT_TEMPLATE.format(year=YEAR) + f"\n\n[Début Bloc {block_id}]\n\n{chunk}"
    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[{"role": "system", "content": "Tu es un analyste rigoureux des échanges familiaux."},
                 {"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content.strip()

# === Écriture rapport ===
def save_report(results, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        for result in results:
            f.write(result + "\n\n")

# === Programme principal ===
def main():
    print(f"🔍 Analyse des emails de {YEAR}...")
    chunks = read_chunks(INPUT_FILE, CHUNK_SIZE)
    print(f"✂️ {len(chunk)} blocs à traiter")
    results = []
    for i, chunk in enumerate(chunks, 1):
        print(f"➡️ Bloc {i}/{len(chunks)}")
        result = analyze_chunk(chunk, i)
        results.append(result)
    save_report(results, OUTPUT_FILE)
    print(f"✅ Rapport généré : {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
