#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper configurable pour jeux vidéo.
- Respecte robots.txt (lecture simple)
- User-Agent propre
- Retries et timeouts
- Parsers basés sur des sélecteurs CSS pour extraire: title, platform(s), genres, release_date, rating (si dispo), game_id (hash si absent)

Usage:
    from scraper import scrape_all_sources
    rows = scrape_all_sources("scrape_sources.json", limit_per_source=50)
"""

import json
import time
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

DEFAULT_UA = "GameETLBot/1.0 (+https://example.local/etl)"
DEFAULT_TIMEOUT = 20
DEFAULT_RETRIES = 3
PAUSE_BETWEEN_REQUESTS = 1.0  # politeness

@dataclass
class SourceCfg:
    name: str
    base_url: str
    list_urls: List[str]
    item_selector: str
    fields: Dict[str, str]  # CSS selectors for fields
    constant_fields: Dict[str, str]  # constant values to add (e.g., platform)

def _allowed_by_robots(base_url: str, user_agent: str = "*") -> bool:
    """Very simple robots.txt check. If fetch fails, defaults to True."""
    try:
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        r = requests.get(robots_url, timeout=10, headers={"User-Agent": DEFAULT_UA})
        if r.status_code != 200:
            return True
        disallow_lines = [line.split(":")[1].strip()
                          for line in r.text.splitlines()
                          if line.lower().startswith("disallow:")]
        # naive allow: if homepage "/" disallowed, skip
        return "/" not in disallow_lines
    except Exception:
        return True

def _get(url: str, retries: int = DEFAULT_RETRIES, timeout: int = DEFAULT_TIMEOUT) -> Optional[requests.Response]:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": DEFAULT_UA})
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            time.sleep(attempt)  # backoff
    if last_exc:
        raise last_exc
    return None

def _text_or_none(node) -> Optional[str]:
    if node is None:
        return None
    t = node.get_text(strip=True)
    return t or None

def _select_text(soup: BeautifulSoup, selector: str) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    return _text_or_none(el)

def _hash_id(parts: List[Optional[str]]) -> int:
    data = "|".join([p or "" for p in parts]).encode("utf-8")
    h = hashlib.sha256(data).hexdigest()[:16]
    # convert hex to int for storage as BIGINT (within range using a slice)
    return int(h, 16) % (2**63 - 1)

def parse_list_page(html: str, cfg: SourceCfg) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(cfg.item_selector)
    results: List[Dict[str, Any]] = []

    for item in items:
        # Build a minimal soup for extraction (the item itself)
        s = BeautifulSoup(str(item), "html.parser")
        title = None
        if "title" in cfg.fields:
            title = _select_text(s, cfg.fields["title"])
        # Try link href for id
        href = None
        link_sel = cfg.fields.get("link")
        if link_sel:
            link_el = s.select_one(link_sel)
            if link_el and link_el.has_attr("href"):
                href = link_el["href"]

        platforms = _select_text(s, cfg.fields.get("platforms", "")) if "platforms" in cfg.fields else None
        genres = _select_text(s, cfg.fields.get("genres", "")) if "genres" in cfg.fields else None
        release_date = _select_text(s, cfg.fields.get("release_date", "")) if "release_date" in cfg.fields else None
        rating = _select_text(s, cfg.fields.get("rating", "")) if "rating" in cfg.fields else None
        # normalize rating to float if possible
        rating_val = None
        if rating:
            try:
                rating_val = float(rating.replace(",", ".").split("/")[0])
            except Exception:
                rating_val = None

        # Fallback platform/genres from constants
        if not platforms and "platforms" in cfg.constant_fields:
            platforms = cfg.constant_fields["platforms"]
        if not genres and "genres" in cfg.constant_fields:
            genres = cfg.constant_fields["genres"]

        # Compute a stable id
        gid = _hash_id([cfg.name, title, href, release_date])

        results.append({
            "game_id": gid,
            "title": title,
            "platforms": platforms,
            "genres": genres,
            "release_date": release_date,
            "rating": rating_val,
            "source": f"scrape:{cfg.name}",
            "raw_json": None,  # could keep the HTML snippet if desired
        })
    return results

def scrape_source(cfg: SourceCfg, limit: int = 50) -> List[Dict[str, Any]]:
    if not _allowed_by_robots(cfg.base_url):
        return []

    out: List[Dict[str, Any]] = []
    for url in cfg.list_urls:
        resp = _get(url)
        time.sleep(PAUSE_BETWEEN_REQUESTS)
        rows = parse_list_page(resp.text, cfg)
        out.extend(rows)
        if len(out) >= limit:
            break
    return out[:limit]

def load_sources(path: str) -> List[SourceCfg]:
    data = json.load(open(path, "r", encoding="utf-8"))
    cfgs: List[SourceCfg] = []
    for s in data.get("sources", []):
        cfgs.append(SourceCfg(
            name=s["name"],
            base_url=s["base_url"],
            list_urls=s["list_urls"],
            item_selector=s["item_selector"],
            fields=s.get("fields", {}),
            constant_fields=s.get("constant_fields", {}),
        ))
    return cfgs

def scrape_all_sources(config_path: str, limit_per_source: int = 50) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    cfgs = load_sources(config_path)
    for cfg in cfgs:
        try:
            part = scrape_source(cfg, limit=limit_per_source)
            rows.extend(part)
        except Exception as e:
            # continue on error per source
            continue
    return rows
