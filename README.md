# ETL Jeux Vidéo — API + Scraping + Mise à jour au démarrage

## Contenu
- `etl_games.py` — Script principal ETL (RAWG API + Scraping optionnel) avec UPSERT MySQL.
- `scraper.py` — Scraper configurable (robots.txt, UA, retries, timeouts).
- `scrape_sources.json.example` — Exemple de configuration des sources à scraper.
- `.env.example` — Modèle d’environnement (clé RAWG + accès MySQL + flags scraping).
- `requirements.txt` — Dépendances Python.
- `run_etl.bat` — Lance l’ETL (Windows).
- `run_etl.sh` — Lance l’ETL (Linux/macOS).
- `etl-games.service` — Service systemd pour lancer au boot (Linux).
- `etl_task_at_logon.xml` — Tâche Planificateur Windows pour lancer à l’ouverture de session.

## Installation
```bash
# 1) Créer un venv et installer les dépendances
python -m venv .venv
# Windows
.venv\Scripts\pip install -r requirements.txt
# Linux/macOS
.venv/bin/pip install -r requirements.txt

# 2) Configurer l'environnement
cp .env.example .env
# Éditer .env : RAWG_API_KEY, DB_USER, DB_PASSWORD, DB_HOST, DB_NAME
# (optionnel scraping) SCRAPE_ENABLED=true ; SCRAPE_CONFIG=scrape_sources.json

# 3) Configurer les sources de scraping (facultatif)
cp scrape_sources.json.example scrape_sources.json
# Éditer les sélecteurs CSS pour tes sources autorisées
```

## Exécution manuelle
```bash
# API + Scraping (si activé dans .env)
# Windows
.venv\Scripts\python etl_games.py --limit 50
# Linux/macOS
.venv/bin/python etl_games.py --limit 50

# Scraping uniquement
python etl_games.py --limit 50 --no-api
```

## Exécution automatique au démarrage

### Windows — Planificateur de tâches
1. Placer les fichiers dans `%USERPROFILE%\etl` (ou modifier le XML en conséquence).
2. Importer `etl_task_at_logon.xml` dans le Planificateur de tâches.
3. Vérifier que `WorkingDirectory` et la commande pointent vers `run_etl.bat`.

### Linux — systemd
```bash
sudo cp etl-games.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable etl-games.service
# (adapter ExecStart si utilisation d'un venv)
```

## Notes
- Le script est **idempotent** : `INSERT ... ON DUPLICATE KEY UPDATE`.
- Table créée automatiquement si absente (par défaut `games`).
- Respecte les bonnes pratiques de scraping (robots.txt, politeness). Utiliser les APIs officielles quand c’est possible.
- Logs dans `etl_games.log`.
