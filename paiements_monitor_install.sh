#!/bin/bash

# INTALLATION: sur serveur distant taper:
# chmod +x install.sh
# ./install.sh

echo "🚀 Démarrage de l'installation du système de suivi de paiements"

# === Dossiers nécessaires ===
echo "📁 Création des dossiers..."
mkdir -p logs
mkdir -p archives_emails

# === Fichier CSV s'il n'existe pas ===
CSV_FILE="paiements.csv"
if [ ! -f "$CSV_FILE" ]; then
    echo "📄 Création de $CSV_FILE..."
    echo "date,paiement,due,note" > "$CSV_FILE"
fi

# === Fichier .env s'il n'existe pas ===
if [ ! -f ".env" ]; then
    echo "📄 Copie de .env.example -> .env"
    cp .env.example .env
    echo "⚠️  Remplis le fichier .env avec tes informations Gmail et API."
fi

# === Installation des dépendances ===
echo "📦 Installation des dépendances Python..."
python3 -m pip install --upgrade pip
pip3 install python-dotenv imapclient pyzmail36 openai

# === Test du script principal ===
echo "🧪 Test rapide de paiement_monitor.py..."
python3 paiement_monitor.py >> logs/monitor_install_test.log 2>&1

echo "✅ Installation terminée."

# === Proposer cron automatique ===
read -p "Souhaites-tu ajouter le script en tâche CRON toutes les 2h ? (y/n) " cronrep
if [[ "$cronrep" == "y" ]]; then
    CRONLINE="0 */2 * * * /usr/bin/python3 $(pwd)/paiement_monitor.py >> $(pwd)/logs/monitor.log 2>&1"
    (crontab -l 2>/dev/null; echo "$CRONLINE") | crontab -
    echo "🕒 Tâche CRON ajoutée."
else
    echo "⏭ Tâche CRON ignorée pour l’instant."
fi

