#!/usr/bin/env python3
"""
ETL Jeux Vidéo — démarre au boot, ajoute 50 nouveaux jeux et met à jour les existants.

👉 Remplissez .env (voir .env.example) puis lancez:
    python etl_games.py --limit 50

Par défaut:
- Source: RAWG.io (gratuite, nécessite une clé API)
- DB: MySQL/MariaDB

Le script est idempotent : il fait des UPSERTS (INSERT ... ON DUPLICATE KEY UPDATE).
Vous pouvez le relancer autant de fois que nécessaire (ex: au démarrage du PC).
"""

import os
import sys
import time
import json
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =====================
# Config & Logging
# =====================

def setup_logging(level: str = "INFO") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("etl_games.log", encoding="utf-8")
        ],
    )


def load_config() -> Dict[str, str]:
    load_dotenv()
    cfg = {
        "RAWG_API_KEY": os.getenv("RAWG_API_KEY", ""),
        "DB_USER": os.getenv("DB_USER", "root"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", ""),
        "DB_HOST": os.getenv("DB_HOST", "127.0.0.1"),
        "DB_PORT": os.getenv("DB_PORT", "3306"),
        "DB_NAME": os.getenv("DB_NAME", "games_db"),
        "DB_TABLE": os.getenv("DB_TABLE", "games"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "HTTP_TIMEOUT": os.getenv("HTTP_TIMEOUT", "20"),
        "HTTP_RETRIES": os.getenv("HTTP_RETRIES", "3"),
    }
    return cfg


# =====================
# DB helpers
# =====================

def make_engine(cfg: Dict[str, str]) -> Engine:
    uri = f"mysql+pymysql://{cfg['DB_USER']}:{cfg['DB_PASSWORD']}@{cfg['DB_HOST']}:{cfg['DB_PORT']}/{cfg['DB_NAME']}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True, pool_recycle=3600, future=True)
    return engine


DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS `{table}` (
  `game_id` BIGINT NOT NULL,
  `title` VARCHAR(255) NOT NULL,
  `platforms` VARCHAR(255) NULL,
  `genres` VARCHAR(255) NULL,
  `release_date` DATE NULL,
  `rating` DECIMAL(4,2) NULL,
  `source` VARCHAR(50) NOT NULL DEFAULT 'rawg',
  `raw_json` JSON NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`game_id`),
  KEY `idx_title` (`title`),
  KEY `idx_release_date` (`release_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SQL = """
INSERT INTO `{table}`
(`game_id`, `title`, `platforms`, `genres`, `release_date`, `rating`, `source`, `raw_json`)
VALUES
(:game_id, :title, :platforms, :genres, :release_date, :rating, :source, CAST(:raw_json AS JSON))
ON DUPLICATE KEY UPDATE
  `title` = VALUES(`title`),
  `platforms` = VALUES(`platforms`),
  `genres` = VALUES(`genres`),
  `release_date` = VALUES(`release_date`),
  `rating` = VALUES(`rating`),
  `source` = VALUES(`source`),
  `raw_json` = VALUES(`raw_json`);
"""


def ensure_table(engine: Engine, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL_CREATE_TABLE.format(table=table)))
    logging.info("✅ Table '%s' prête.", table)


def upsert_rows(engine: Engine, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        stmt = text(UPSERT_SQL.format(table=table))
        conn.execute(stmt, rows)
    return len(rows)


# =====================
# RAWG fetcher
# =====================

def rawg_fetch_latest(api_key: str, limit: int, timeout: int, retries: int) -> List[Dict[str, Any]]:
    """
    Récupère les derniers jeux sortis ou récemment référencés.
    Docs RAWG: https://rawg.io/apidocs
    Endpoint: GET /games?ordering=-released&page_size={limit}
    """
    if not api_key:
        raise RuntimeError("RAWG_API_KEY manquant. Renseignez-le dans .env")

    url = "https://api.rawg.io/api/games"
    params = {
        "key": api_key,
        "page_size": min(max(limit, 1), 100),
        "ordering": "-released",
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                # Rate limit: attendre et retenter
                wait = int(resp.headers.get("Retry-After", "10"))
                logging.warning("🕒 Rate limited (429). Attente %ss puis retry...", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            normalized = []
            for g in results[:limit]:
                game_id = g.get("id")
                title = g.get("name")
                release_date = g.get("released") or None
                platforms = ", ".join([p["platform"]["name"] for p in g.get("platforms", []) if p.get("platform")])
                genres = ", ".join([p["name"] for p in g.get("genres", [])])
                rating = g.get("rating")
                normalized.append({
                    "game_id": game_id,
                    "title": title[:255] if title else None,
                    "platforms": platforms[:255] if platforms else None,
                    "genres": genres[:255] if genres else None,
                    "release_date": release_date,
                    "rating": rating,
                    "source": "rawg",
                    "raw_json": json.dumps(g, ensure_ascii=False),
                })
            return normalized
        except Exception as e:
            last_exc = e
            logging.error("Essai %d/%d échoué: %s", attempt, retries, e)
            time.sleep(2 * attempt)

    assert last_exc is not None
    raise last_exc



# =====================
# Optional: Web scraping
# =====================
def maybe_scrape_and_merge(limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        if os.getenv("SCRAPE_ENABLED", "false").lower() in {"1", "true", "yes"}:
            cfg_path = os.getenv("SCRAPE_CONFIG", "scrape_sources.json")
            from scraper import scrape_all_sources
            logging.info("🕷️ Scraping activé. Lecture des sources depuis %s ...", cfg_path)
            srows = scrape_all_sources(cfg_path, limit_per_source=limit)
            logging.info("🕷️ Scraping: %d lignes récupérées.", len(srows))
            rows.extend(srows)
        else:
            logging.info("🕷️ Scraping désactivé (SCRAPE_ENABLED=false).")
    except Exception as e:
        logging.warning("Scraping ignoré: %s", e)
    return rows

# =====================
# Main flow
# =====================

def main() -> int:
    cfg = load_config()
    setup_logging(cfg.get("LOG_LEVEL", "INFO"))

    parser = argparse.ArgumentParser(description="ETL Jeux Vidéo — ajoute 50 nouveaux jeux et met à jour les existants.")
    parser.add_argument("--limit", type=int, default=50, help="Nombre de nouveaux jeux à récupérer (défaut: 50)")
    parser.add_argument("--no-api", action="store_true", help="Ne pas utiliser l'API RAWG, uniquement scraping")
    args = parser.parse_args()

    limit = args.limit
    timeout = int(cfg["HTTP_TIMEOUT"])
    retries = int(cfg["HTTP_RETRIES"])

    try:
        engine = make_engine(cfg)
        ensure_table(engine, cfg["DB_TABLE"])

        logging.info("📥 Récupération des %d derniers jeux depuis RAWG...", limit)
        rows = []
        # API RAWG (unless disabled)
        if not args.no_api:
            logging.info("📥 Récupération des %d derniers jeux depuis RAWG...", limit)
            api_rows = rawg_fetch_latest(cfg["RAWG_API_KEY"], limit=limit, timeout=timeout, retries=retries)
            logging.info("🔢 RAWG: %d jeux récupérés.", len(api_rows))
            rows.extend(api_rows)
        else:
            logging.info("⏭️ API RAWG désactivée (--no-api).")

        # Scraping (optional)
        scrape_rows = maybe_scrape_and_merge(limit)
        rows.extend(scrape_rows)

        # Dedup by game_id keeping last
        dedup = {}
        for r in rows:
            if r.get("game_id") is None:
                # skip invalid rows
                continue
            dedup[r["game_id"]] = r
        rows = list(dedup.values())
        logging.info("📦 Total à upserter: %d lignes après dédup.", len(rows))

        n = upsert_rows(engine, cfg["DB_TABLE"], rows)
        logging.info("📤 UPSERT terminé: %d lignes insérées/mises à jour.", n)
        logging.info("✅ Terminé.")

        return 0
    except Exception as e:
        logging.exception("❌ ETL échoué: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
