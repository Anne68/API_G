#!/usr/bin/env bash
# Lancer l'ETL jeux vidéo (Linux/macOS)
set -euo pipefail

# Aller dans le dossier du script
cd "$(dirname "$0")"

# Activer venv si présent
if [ -f ".venv/bin/activate" ]; then
  source ".venv/bin/activate"
fi

python3 etl_games.py --limit 50
