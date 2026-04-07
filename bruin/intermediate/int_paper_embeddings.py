"""Bruin Python asset: int_paper_embeddings

Computes TF-IDF topic vectors for all staged papers and writes
sparse topic features + top-keyword assignments back to PostgreSQL.

This is the ML component that turns unstructured abstracts into
structured topic signals for clustering and the topic heatmap.

Depends: staging.stg_papers
Writes:  intermediate.int_paper_embeddings
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

DDL_EMBEDDING_TABLE = """
CREATE TABLE IF NOT EXISTS int_paper_embeddings (
    paper_id        TEXT PRIMARY KEY,
    top_keywords    TEXT[],          -- top 10 TF-IDF terms
    top_scores      FLOAT8[],        -- corresponding TF-IDF scores
    topic_label     TEXT,            -- dominant keyword as topic proxy
    abstract_length INT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO int_paper_embeddings
    (paper_id, top_keywords, top_scores, topic_label, abstract_length, updated_at)
VALUES
    (%(paper_id)s, %(top_keywords)s, %(top_scores)s, %(topic_label)s,
    %(abstract_length)s, NOW())
ON CONFLICT (paper_id) DO UPDATE SET
    top_keywords    = EXCLUDED.top_keywords,
    top_scores      = EXCLUDED.top_scores,
    topic_label     = EXCLUDED.topic_label,
    abstract_length = EXCLUDED.abstract_length,
    updated_at      = NOW()
"""

# Common NLP stop words supplement for ML/CS domain
DOMAIN_STOP_WORDS = {
    "paper", "propose", "proposed", "present", "show", "shows",
    "model", "models", "method", "methods", "approach", "approaches",
    "result", "results", "performance", "based", "using", "used",
    "also", "however", "furthermore", "thus", "therefore",
}


@dataclass(frozen=True)
class PaperRecord:
    paper_id: str
    title: str
    abstract: str


def _clean_text(text: str) -> str:
    """Basic text cleaning: lower, strip math/special chars."""
    text = text.lower()
    text = re.sub(r"\$[^$]+\$", " ", text)          # strip inline LaTeX math
    text = re.sub(r"[^a-z\s\-]", " ", text)         # keep letters, spaces, hyphens
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_papers(conn: psycopg2.extensions.connection) -> list[PaperRecord]:
    """Load all papers from staging.stg_papers."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT paper_id, title, abstract
            FROM staging.stg_papers
            WHERE abstract IS NOT NULL AND abstract != ''
            ORDER BY paper_id
            """
        )
        rows = cur.fetchall()
    logger.info("Loaded %d papers for TF-IDF computation", len(rows))
    return [PaperRecord(r["paper_id"], r["title"], r["abstract"]) for r in rows]


def compute_tfidf(
    papers: list[PaperRecord],
    max_features: int = 5000,
    top_k: int = 10,
) -> list[dict[str, Any]]:
    """Fit TF-IDF on title+abstract corpus and extract top keywords per paper."""
    corpus = [
        _clean_text(f"{p.title} {p.abstract}")
        for p in papers
    ]

    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
    stop_words = list(ENGLISH_STOP_WORDS | DOMAIN_STOP_WORDS)

    vectorizer = TfidfVectorizer(
        max_features=max_features,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.85,
        stop_words=stop_words,
        sublinear_tf=True,
    )

    tfidf_matrix: csr_matrix = vectorizer.fit_transform(corpus)
    # L2-normalize rows so cosine similarity = dot product
    tfidf_matrix = normalize(tfidf_matrix, norm="l2")

    feature_names: list[str] = vectorizer.get_feature_names_out().tolist()

    records: list[dict[str, Any]] = []
    for idx, paper in enumerate(papers):
        row = tfidf_matrix[idx]
        # Get non-zero indices sorted by score descending
        cx = row.tocoo()
        if cx.nnz == 0:
            top_kws: list[str] = []
            top_sc: list[float] = []
        else:
            sorted_idx = np.argsort(cx.data)[::-1][:top_k]
            top_kws = [feature_names[cx.col[i]] for i in sorted_idx]
            top_sc = [float(cx.data[i]) for i in sorted_idx]

        records.append(
            {
                "paper_id": paper.paper_id,
                "top_keywords": top_kws,
                "top_scores": top_sc,
                "topic_label": top_kws[0] if top_kws else "unknown",
                "abstract_length": len(paper.abstract.split()),
            }
        )

    logger.info("Computed TF-IDF embeddings for %d papers", len(records))
    return records


def write_embeddings(
    conn: psycopg2.extensions.connection,
    records: list[dict[str, Any]],
) -> None:
    """Write embedding records to int_paper_embeddings table."""
    with conn.cursor() as cur:
        cur.execute(DDL_EMBEDDING_TABLE)
        # Use execute_values for efficiency
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO int_paper_embeddings
                (paper_id, top_keywords, top_scores, topic_label, abstract_length, updated_at)
            VALUES %s
            ON CONFLICT (paper_id) DO UPDATE SET
                top_keywords    = EXCLUDED.top_keywords,
                top_scores      = EXCLUDED.top_scores,
                topic_label     = EXCLUDED.topic_label,
                abstract_length = EXCLUDED.abstract_length,
                updated_at      = NOW()
            """,
            [
                (
                    r["paper_id"],
                    r["top_keywords"],
                    r["top_scores"],
                    r["topic_label"],
                    r["abstract_length"],
                )
                for r in records
            ],
            template="(%s, %s, %s, %s, %s, NOW())",
            page_size=500,
        )
    conn.commit()
    logger.info("Wrote %d embedding records", len(records))


def materialize(conn: psycopg2.extensions.connection) -> None:
    """Main entry point called by Bruin."""
    papers = fetch_papers(conn)
    if not papers:
        logger.warning("No papers found; skipping TF-IDF computation")
        return
    records = compute_tfidf(papers)
    write_embeddings(conn, records)


# Bruin Python asset interface
if __name__ == "__main__":
    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    try:
        materialize(conn)
    finally:
        conn.close()
