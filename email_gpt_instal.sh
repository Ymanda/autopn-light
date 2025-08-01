#!/bin/bash

echo "🚀 Installation du système de réponse automatique GPT"

# === Dossiers nécessaires ===
echo "📁 Création des dossiers..."
mkdir -p logs
mkdir -p archives_emails

# === Fichier de contexte s'il n'existe pas ===
if [ ! -f "base_context.txt" ]; then
    echo "📄 Création de base_context.txt (vide)"
    echo "# Données de base\n\n# Objectifs de réponse" > base_context.txt
fi

# === Fichier .env s'il n'existe pas ===
if [ ! -f ".env" ]; then
    echo "📄 Copie de .env.example -> .env"
    cp .env.example .env
    echo "⚠️  Remplis le fichier .env avec tes infos (Gmail, OpenAI, etc.)."
fi

# === Installation des dépendances Python ===
echo "📦 Installation des dépendances Python..."
python3 -m pip install --upgrade pip
pip3 install python-dotenv imapclient pyzmail36 openai

# === Test du script GPT ===
echo "🧪 Test rapide de email_gpt_reply.py..."
python3 email_gpt_reply.py >> logs/gpt_reply_install_test.log 2>&1

echo "✅ Installation terminée."

# === Proposer ajout CRON (optionnel) ===
read -p "Ajouter ce script en tâche CRON chaque matin à 8h ? (y/n) " cronrep
if [[ "$cronrep" == "y" ]]; then
    CRONLINE="0 8 * * * /usr/bin/python3 $(pwd)/email_gpt_reply.py >> $(pwd)/logs/gpt_reply.log 2>&1"
    (crontab -l 2>/dev/null; echo "$CRONLINE") | crontab -
    echo "🕒 Tâche CRON ajoutée."
else
    echo "⏭ CRON ignoré pour le moment."
fi
