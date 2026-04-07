"""arXiv producer: polls arXiv API and writes raw paper records directly to Supabase."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterator

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

ARXIV_NS = "http://www.w3.org/2005/Atom"
ARXIV_API_BASE = "http://export.arxiv.org/api/query"


@dataclass
class ArxivPaper:
    paper_id: str
    title: str
    abstract: str
    authors: list[str]
    categories: list[str]
    published: str
    updated: str
    doi: str | None
    source: str = "arxiv"
    ingested_at: str = ""

    def __post_init__(self) -> None:
        if not self.ingested_at:
            self.ingested_at = datetime.now(timezone.utc).isoformat()


def _build_query_url(categories: list[str], start: int, max_results: int) -> str:
    """Build arXiv API query URL for given categories."""
    cat_query = " OR ".join(f"cat:{c}" for c in categories)
    params = urllib.parse.urlencode(
        {
            "search_query": cat_query,
            "start": start,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
    )
    return f"{ARXIV_API_BASE}?{params}"


def _parse_feed(xml_content: bytes) -> Iterator[ArxivPaper]:
    """Parse arXiv Atom feed and yield ArxivPaper instances."""
    root = ET.fromstring(xml_content)
    for entry in root.findall(f"{{{ARXIV_NS}}}entry"):
        try:
            paper_id_raw = entry.findtext(f"{{{ARXIV_NS}}}id", default="")
            paper_id = paper_id_raw.split("/abs/")[-1].strip()

            title_el = entry.find(f"{{{ARXIV_NS}}}title")
            title = " ".join((title_el.text or "").split()) if title_el is not None else ""

            summary_el = entry.find(f"{{{ARXIV_NS}}}summary")
            abstract = " ".join((summary_el.text or "").split()) if summary_el is not None else ""

            authors = [
                a.findtext(f"{{{ARXIV_NS}}}name", default="").strip()
                for a in entry.findall(f"{{{ARXIV_NS}}}author")
            ]

            categories = [
                t.get("term", "")
                for t in entry.findall(f"{{{ARXIV_NS}}}category")
            ]

            published = entry.findtext(f"{{{ARXIV_NS}}}published", default="")
            updated = entry.findtext(f"{{{ARXIV_NS}}}updated", default="")

            arxiv_ns2 = "http://arxiv.org/schemas/atom"
            doi_el = entry.find(f"{{{arxiv_ns2}}}doi")
            doi = doi_el.text.strip() if doi_el is not None and doi_el.text else None

            yield ArxivPaper(
                paper_id=paper_id,
                title=title,
                abstract=abstract,
                authors=authors,
                categories=categories,
                published=published,
                updated=updated,
                doi=doi,
            )
        except Exception as exc:
            logger.warning("Failed to parse entry: %s", exc)
            continue


def fetch_papers(categories: list[str], max_results: int) -> list[ArxivPaper]:
    """Fetch papers from arXiv API with retry logic."""
    url = _build_query_url(categories, start=0, max_results=max_results)
    for attempt in range(3):
        try:
            logger.info("Fetching arXiv papers (attempt %d): %s", attempt + 1, url)
            with urllib.request.urlopen(url, timeout=30) as resp:
                content = resp.read()
            papers = list(_parse_feed(content))
            logger.info("Fetched %d papers from arXiv", len(papers))
            return papers
        except urllib.error.URLError as exc:
            logger.warning("arXiv request failed (attempt %d): %s", attempt + 1, exc)
            if attempt < 2:
                time.sleep(2 ** attempt * 5)
    logger.error("All arXiv fetch attempts failed")
    return []


def build_conn() -> psycopg2.extensions.connection:
    """Create and return a psycopg2 connection to Supabase PostgreSQL."""
    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    return conn


def upsert_papers(conn: psycopg2.extensions.connection, papers: list[ArxivPaper]) -> int:
    """Upsert papers into landing.raw_arxiv_papers; return count inserted/updated."""
    if not papers:
        return 0

    rows = [
        (
            p.paper_id,
            p.title,
            p.abstract,
            json.dumps(p.authors),
            json.dumps(p.categories),
            p.published,
            p.updated,
            p.doi,
            p.source,
            p.ingested_at,
        )
        for p in papers
    ]

    sql = """
        INSERT INTO landing.raw_arxiv_papers
            (paper_id, title, abstract, authors, categories,
             published, updated, doi, source, ingested_at)
        VALUES %s
        ON CONFLICT (paper_id) DO UPDATE SET
            title        = EXCLUDED.title,
            abstract     = EXCLUDED.abstract,
            authors      = EXCLUDED.authors,
            categories   = EXCLUDED.categories,
            updated      = EXCLUDED.updated,
            doi          = EXCLUDED.doi,
            ingested_at  = EXCLUDED.ingested_at
    """

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
        conn.commit()
        logger.info("Upserted %d papers into landing.raw_arxiv_papers", len(rows))
        return len(rows)
    except psycopg2.Error as exc:
        conn.rollback()
        logger.error("DB upsert failed: %s", exc)
        raise


def main() -> None:
    """Main ingestion loop: poll arXiv → write directly to Supabase."""
    categories_raw = os.environ.get("ARXIV_CATEGORIES", "cs.AI,cs.LG,q-bio.NC")
    categories = [c.strip() for c in categories_raw.split(",")]
    max_results = int(os.environ.get("ARXIV_MAX_RESULTS", "100"))
    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))

    conn = build_conn()
    logger.info(
        "arXiv producer started | categories=%s max_results=%d poll_interval=%ds",
        categories,
        max_results,
        poll_interval,
    )

    while True:
        try:
            papers = fetch_papers(categories, max_results)
            if papers:
                upsert_papers(conn, papers)
            else:
                logger.warning("No papers fetched this cycle")
        except psycopg2.OperationalError as exc:
            logger.error("DB connection lost, reconnecting: %s", exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = build_conn()
        except Exception as exc:
            logger.error("Unexpected error in producer loop: %s", exc, exc_info=True)

        logger.info("Sleeping %d seconds until next poll", poll_interval)
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
