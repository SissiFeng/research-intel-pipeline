"""Research Intelligence Pipeline — Streamlit dashboard entry point."""

from __future__ import annotations

import logging
import os

import psycopg2
import psycopg2.extras
import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Research Intel Pipeline",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner=False)
def get_connection() -> psycopg2.extensions.connection:
    """Create a cached database connection."""
    db_url = os.environ.get("SUPABASE_DB_URL", "")
    if not db_url:
        st.error("SUPABASE_DB_URL environment variable is not set.")
        st.stop()
    try:
        conn = psycopg2.connect(db_url, connect_timeout=10)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as exc:
        st.error(f"Database connection failed: {exc}")
        st.stop()


def run_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.Error as exc:
        logger.error("Query failed: %s\nSQL: %s", exc, sql)
        st.error(f"Query error: {exc}")
        return []


# Make run_query available to page modules via session state
st.session_state["run_query"] = run_query

# ── Sidebar navigation hint ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔬 Research Intel")
    st.markdown(
        """
        Navigate using the pages above:
        - **Overview** — Today's pulse
        - **Trends** — Topic velocity over time
        - **Citation Network** — Paper-paper graph
        - **Author Network** — Co-authorship graph
        - **Search** — Full-text paper search
        """
    )
    st.divider()
    st.caption("Data: arXiv · OpenAlex · Semantic Scholar")
    st.caption("Pipeline: Bruin + Redpanda + Supabase")

# ── Landing page content ──────────────────────────────────────────────────────
st.title("🔬 Research Intelligence Pipeline")
st.markdown(
    """
    Real-time scientific research analytics for **AI, ML, and Computational Biology**.

    Use the sidebar to navigate to any dashboard page.
    """
)

col1, col2, col3 = st.columns(3)

total_papers = run_query("SELECT COUNT(*) AS n FROM staging.stg_papers")
total_authors = run_query("SELECT COUNT(*) AS n FROM staging.stg_authors")
rising = run_query("SELECT COUNT(*) AS n FROM marts.mart_rising_papers WHERE is_rising = TRUE")

with col1:
    n = total_papers[0]["n"] if total_papers else 0
    st.metric("Total Papers Indexed", f"{n:,}")

with col2:
    n = total_authors[0]["n"] if total_authors else 0
    st.metric("Authors Tracked", f"{n:,}")

with col3:
    n = rising[0]["n"] if rising else 0
    st.metric("Rising Papers (90d)", f"{n:,}")

st.info("Select a page from the sidebar to explore the data.")
