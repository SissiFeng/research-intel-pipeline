"""Dashboard page 5: Search — full-text search over paper abstracts with topic filter."""

from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Search — Research Intel", page_icon="🔍", layout="wide")

run_query = st.session_state.get("run_query")
if run_query is None:
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
    from app import run_query  # type: ignore[assignment]

st.title("🔍 Paper Search")
st.caption("Full-text search over titles and abstracts with topic and category filters")

# ── Search form ───────────────────────────────────────────────────────────────
with st.form("search_form"):
    col_query, col_submit = st.columns([5, 1])
    with col_query:
        query = st.text_input(
            "Search query",
            placeholder="e.g. transformer attention mechanism reinforcement learning",
            label_visibility="collapsed",
        )
    with col_submit:
        submitted = st.form_submit_button("Search", use_container_width=True)

col_cat, col_year_min, col_year_max, col_min_cit = st.columns(4)

with col_cat:
    categories_raw = run_query(
        "SELECT DISTINCT primary_category FROM staging.stg_papers WHERE primary_category IS NOT NULL ORDER BY primary_category"
    )
    all_cats = ["(all)"] + [r["primary_category"] for r in (categories_raw or [])]
    category = st.selectbox("Category", options=all_cats)

with col_year_min:
    year_min = st.number_input("Year from", min_value=2010, max_value=2026, value=2022, step=1)

with col_year_max:
    year_max = st.number_input("Year to", min_value=2010, max_value=2026, value=2026, step=1)

with col_min_cit:
    min_citations = st.number_input("Min citations", min_value=0, value=0, step=1)

# ── Execute search ────────────────────────────────────────────────────────────
if submitted and query.strip():
    # Build WHERE clauses as a list — no f-string interpolation of user-controlled values
    where_parts = [
        "TO_TSVECTOR('english', COALESCE(p.title, '') || ' ' || COALESCE(p.abstract, '')) @@ PLAINTO_TSQUERY('english', %(query)s)",
        "p.published_year >= %(year_min)s",
        "p.published_year <= %(year_max)s",
        "p.cited_by_count >= %(min_citations)s",
    ]
    params: dict[str, object] = {
        "query": query.strip(),
        "year_min": year_min,
        "year_max": year_max,
        "min_citations": min_citations,
    }
    if category != "(all)":
        where_parts.append("p.primary_category = %(category)s")
        params["category"] = category

    where_clause = "\n        AND ".join(where_parts)

    search_sql = f"""
    SELECT
        p.paper_id,
        p.title,
        p.abstract,
        p.primary_category,
        p.published_date,
        p.published_year,
        p.cited_by_count,
        p.author_count,
        p.doi,
        tc.topic_label,
        TS_RANK(
            TO_TSVECTOR('english', COALESCE(p.title, '') || ' ' || COALESCE(p.abstract, '')),
            PLAINTO_TSQUERY('english', %(query)s)
        ) AS relevance
    FROM staging.stg_papers p
    LEFT JOIN intermediate.int_topic_clusters tc ON p.paper_id = tc.paper_id
    WHERE
        {where_clause}
    ORDER BY relevance DESC, p.cited_by_count DESC
    LIMIT 50
    """

    @st.cache_data(ttl=300, show_spinner="Searching...")
    def do_search(sql: str, search_params: dict[str, object]) -> list[dict]:  # type: ignore[type-arg]
        """Cached search keyed by SQL and params dict."""
        return run_query(sql, search_params)  # type: ignore[arg-type]

    results = do_search(search_sql, params)

    if results:
        st.success(f"Found **{len(results)}** papers (showing top 50 by relevance)")
        df = pd.DataFrame(results)
        df["arxiv_url"] = "https://arxiv.org/abs/" + df["paper_id"]
        df["relevance"] = df["relevance"].apply(lambda x: f"{float(x):.4f}")

        # Tabular view
        st.dataframe(
            df[["title", "primary_category", "topic_label", "published_date",
                "cited_by_count", "author_count", "relevance", "arxiv_url"]],
            use_container_width=True,
            column_config={
                "arxiv_url": st.column_config.LinkColumn("arXiv"),
                "title": st.column_config.TextColumn("Title", width="large"),
                "relevance": st.column_config.TextColumn("Relevance"),
                "cited_by_count": st.column_config.NumberColumn("Citations"),
                "author_count": st.column_config.NumberColumn("Authors"),
            },
            hide_index=True,
        )

        # Card view for top 5
        st.divider()
        st.subheader("Top 5 Results")
        for row in results[:5]:
            with st.expander(f"📄 {row['title']}", expanded=False):
                col_meta, col_abs = st.columns([1, 2])
                with col_meta:
                    st.write(f"**Category:** {row['primary_category']}")
                    st.write(f"**Topic:** {row.get('topic_label', '—')}")
                    st.write(f"**Published:** {row['published_date']}")
                    st.write(f"**Citations:** {row['cited_by_count']}")
                    st.write(f"**Authors:** {row['author_count']}")
                    st.write(f"[Open on arXiv →](https://arxiv.org/abs/{row['paper_id']})")
                with col_abs:
                    abstract = row.get("abstract", "")
                    # Highlight query terms in abstract
                    display_abstract = abstract[:800] + ("…" if len(abstract) > 800 else "")
                    st.write(display_abstract)
    else:
        st.warning("No papers matched your search. Try different keywords.")
elif submitted and not query.strip():
    st.warning("Please enter a search query.")
else:
    # Show recent papers as default state
    st.subheader("Recent Papers")
    recent = run_query(
        """
        SELECT paper_id, title, primary_category, published_date, cited_by_count
        FROM staging.stg_papers
        ORDER BY published_date DESC NULLS LAST
        LIMIT 20
        """
    )
    if recent:
        df_recent = pd.DataFrame(recent)
        df_recent["arxiv_url"] = "https://arxiv.org/abs/" + df_recent["paper_id"]
        st.dataframe(
            df_recent[["title", "primary_category", "published_date", "cited_by_count", "arxiv_url"]],
            use_container_width=True,
            column_config={
                "arxiv_url": st.column_config.LinkColumn("arXiv"),
                "title": st.column_config.TextColumn("Title", width="large"),
            },
            hide_index=True,
        )
    else:
        st.info("No papers indexed yet. Run the pipeline to ingest data.")
