"""arXiv paper producer — polls arXiv API and writes to Supabase via REST."""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import structlog

log = structlog.get_logger()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
ARXIV_API = "http://export.arxiv.org/api/query"
CATEGORIES = ["cs.AI", "cs.LG", "cs.MA", "q-bio.QM"]
MAX_RESULTS = 50
POLL_INTERVAL = 900  # 15 minutes

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


def fetch_arxiv(category: str, max_results: int = MAX_RESULTS) -> list[dict]:
    """Fetch recent papers from arXiv for a given category."""
    params = urllib.parse.urlencode({
        "search_query": f"cat:{category}",
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    })
    url = f"{ARXIV_API}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "research-intel-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        content = r.read()

    root = ET.fromstring(content)
    papers = []
    for entry in root.findall("atom:entry", NS):
        arxiv_id = entry.findtext("atom:id", namespaces=NS, default="")
        arxiv_id = arxiv_id.split("/abs/")[-1].strip()

        authors = [
            a.findtext("atom:name", namespaces=NS, default="")
            for a in entry.findall("atom:author", NS)
        ]
        categories = [
            t.get("term", "")
            for t in entry.findall("atom:category", NS)
        ]
        published = entry.findtext("atom:published", namespaces=NS, default="")
        updated = entry.findtext("atom:updated", namespaces=NS, default="")

        papers.append({
            "id": arxiv_id,
            "title": (entry.findtext("atom:title", namespaces=NS, default="") or "").strip(),
            "abstract": (entry.findtext("atom:summary", namespaces=NS, default="") or "").strip(),
            "authors": authors,
            "categories": categories,
            "published_date": published,
            "updated_date": updated,
            "doi": entry.findtext("arxiv:doi", namespaces=NS),
            "journal_ref": entry.findtext("arxiv:journal_ref", namespaces=NS),
        })
    return papers


def upsert_papers(papers: list[dict]) -> int:
    """Upsert papers to Supabase via REST API."""
    if not papers:
        return 0

    url = f"{SUPABASE_URL}/rest/v1/raw_arxiv_papers"
    payload = json.dumps(papers).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
        return len(papers)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        log.error("upsert_failed", status=e.code, body=body[:200])
        return 0


def run_once() -> None:
    """Run one ingestion cycle across all categories."""
    total = 0
    for cat in CATEGORIES:
        try:
            papers = fetch_arxiv(cat)
            n = upsert_papers(papers)
            log.info("ingested", category=cat, count=n)
            total += n
            time.sleep(3)  # be polite to arXiv API
        except Exception as exc:
            log.error("fetch_error", category=cat, error=str(exc))
    log.info("cycle_complete", total=total, timestamp=datetime.now(timezone.utc).isoformat())


def main() -> None:
    log.info("arxiv_producer_start", categories=CATEGORIES, interval=POLL_INTERVAL)
    while True:
        run_once()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
