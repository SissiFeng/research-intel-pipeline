"""OpenAlex producer: polls OpenAlex API and writes citation/work records directly to Supabase."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

OPENALEX_API_BASE = "https://api.openalex.org/works"
# polite pool: include email in mailto param
MAILTO = os.environ.get("OPENALEX_MAILTO", "research-pipeline@example.com")


@dataclass
class OpenAlexWork:
    work_id: str
    doi: str | None
    title: str
    publication_year: int | None
    publication_date: str | None
    cited_by_count: int
    concepts: list[dict[str, Any]]
    authorships: list[dict[str, Any]]
    referenced_works: list[str]
    source: str = "openalex"
    ingested_at: str = ""

    def __post_init__(self) -> None:
        if not self.ingested_at:
            self.ingested_at = datetime.now(timezone.utc).isoformat()


def _build_query_url(filter_str: str, cursor: str, per_page: int) -> str:
    """Build OpenAlex paginated query URL."""
    params: dict[str, str | int] = {
        "filter": filter_str,
        "per-page": per_page,
        "cursor": cursor,
        "select": (
            "id,doi,title,publication_year,publication_date,"
            "cited_by_count,concepts,authorships,referenced_works"
        ),
        "mailto": MAILTO,
    }
    return f"{OPENALEX_API_BASE}?{urllib.parse.urlencode(params)}"


def _parse_work(raw: dict[str, Any]) -> OpenAlexWork | None:
    """Parse a single OpenAlex work dict into OpenAlexWork dataclass."""
    try:
        work_id = raw.get("id", "")
        doi_raw = raw.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "").strip() or None

        concepts = [
            {
                "id": c.get("id", ""),
                "display_name": c.get("display_name", ""),
                "level": c.get("level", 0),
                "score": c.get("score", 0.0),
            }
            for c in (raw.get("concepts") or [])
        ]

        authorships = [
            {
                "author_id": a.get("author", {}).get("id", ""),
                "author_name": a.get("author", {}).get("display_name", ""),
                "institutions": [
                    i.get("display_name", "") for i in (a.get("institutions") or [])
                ],
                "position": a.get("author_position", ""),
            }
            for a in (raw.get("authorships") or [])
        ]

        referenced_works = [w for w in (raw.get("referenced_works") or [])]

        return OpenAlexWork(
            work_id=work_id,
            doi=doi,
            title=raw.get("title") or "",
            publication_year=raw.get("publication_year"),
            publication_date=raw.get("publication_date"),
            cited_by_count=raw.get("cited_by_count") or 0,
            concepts=concepts,
            authorships=authorships,
            referenced_works=referenced_works,
        )
    except Exception as exc:
        logger.warning("Failed to parse OpenAlex work %s: %s", raw.get("id"), exc)
        return None


def fetch_works(max_results: int) -> list[OpenAlexWork]:
    """Fetch works from OpenAlex API using cursor-based pagination."""
    # Filter: AI / ML concepts from 2023 onwards
    filter_str = (
        "concepts.id:C154945302|C119857082,"  # machine learning | artificial intelligence
        "from_publication_date:2023-01-01,"
        "type:article"
    )
    per_page = min(200, max_results)
    cursor = "*"
    works: list[OpenAlexWork] = []

    while len(works) < max_results:
        url = _build_query_url(filter_str, cursor, per_page)
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": f"ResearchPipeline/1.0 (mailto:{MAILTO})"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data: dict[str, Any] = json.loads(resp.read())
                break
            except urllib.error.URLError as exc:
                logger.warning("OpenAlex request failed (attempt %d): %s", attempt + 1, exc)
                if attempt < 2:
                    time.sleep(2 ** attempt * 5)
        else:
            logger.error("All OpenAlex fetch attempts failed for cursor=%s", cursor)
            break

        results = data.get("results", [])
        if not results:
            break

        for raw in results:
            work = _parse_work(raw)
            if work:
                works.append(work)
            if len(works) >= max_results:
                break

        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        cursor = next_cursor

        # Polite delay
        time.sleep(0.2)

    logger.info("Fetched %d works from OpenAlex", len(works))
    return works


def build_conn() -> psycopg2.extensions.connection:
    """Create and return a psycopg2 connection to Supabase PostgreSQL."""
    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    return conn


def upsert_works(conn: psycopg2.extensions.connection, works: list[OpenAlexWork]) -> int:
    """Upsert works into landing.raw_openalex_works; return count inserted/updated."""
    if not works:
        return 0

    rows = [
        (
            w.work_id,
            w.doi,
            w.title,
            w.publication_year,
            w.publication_date,
            w.cited_by_count,
            json.dumps(w.concepts),
            json.dumps(w.authorships),
            json.dumps(w.referenced_works),
            w.source,
            w.ingested_at,
        )
        for w in works
    ]

    sql = """
        INSERT INTO landing.raw_openalex_works
            (work_id, doi, title, publication_year, publication_date,
             cited_by_count, concepts, authorships, referenced_works,
             source, ingested_at)
        VALUES %s
        ON CONFLICT (work_id) DO UPDATE SET
            doi              = EXCLUDED.doi,
            title            = EXCLUDED.title,
            cited_by_count   = EXCLUDED.cited_by_count,
            concepts         = EXCLUDED.concepts,
            authorships      = EXCLUDED.authorships,
            referenced_works = EXCLUDED.referenced_works,
            ingested_at      = EXCLUDED.ingested_at
    """

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
        conn.commit()
        logger.info("Upserted %d works into landing.raw_openalex_works", len(rows))
        return len(rows)
    except psycopg2.Error as exc:
        conn.rollback()
        logger.error("DB upsert failed: %s", exc)
        raise


def main() -> None:
    """Main ingestion loop: poll OpenAlex → write directly to Supabase."""
    max_results = int(os.environ.get("OPENALEX_MAX_RESULTS", "200"))
    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "3600"))

    conn = build_conn()
    logger.info(
        "OpenAlex producer started | max_results=%d poll_interval=%ds",
        max_results,
        poll_interval,
    )

    while True:
        try:
            works = fetch_works(max_results)
            if works:
                upsert_works(conn, works)
            else:
                logger.warning("No works fetched this cycle")
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
