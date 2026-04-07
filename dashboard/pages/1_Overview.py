"""Dashboard page 1: Overview — today's pulse, top topics, rising papers feed."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Overview — Research Intel", page_icon="📊", layout="wide")

run_query = st.session_state.get("run_query")
if run_query is None:
    # Fallback: allow running page standalone
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
    from app import run_query  # type: ignore[assignment]

st.title("📊 Overview")
st.caption("Research pulse — updated on each pipeline run")

# ── Top KPI row ───────────────────────────────────────────────────────────────
today_papers = run_query(
    "SELECT COUNT(*) AS n FROM staging.stg_papers WHERE published_date = CURRENT_DATE"
)
week_papers = run_query(
    "SELECT COUNT(*) AS n FROM staging.stg_papers WHERE published_date >= CURRENT_DATE - 7"
)
rising_count = run_query(
    "SELECT COUNT(*) AS n FROM marts.mart_rising_papers WHERE is_rising = TRUE"
)
total_papers = run_query("SELECT COUNT(*) AS n FROM staging.stg_papers")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Papers Today", today_papers[0]["n"] if today_papers else 0)
k2.metric("Papers This Week", week_papers[0]["n"] if week_papers else 0)
k3.metric("Rising Papers", rising_count[0]["n"] if rising_count else 0, delta="90-day window")
k4.metric("Total Indexed", f"{total_papers[0]['n']:,}" if total_papers else 0)

st.divider()

# ── Top topics bar chart ──────────────────────────────────────────────────────
st.subheader("Top Topics by Paper Count (Last 30 Days)")

top_topics = run_query(
    """
    SELECT topic_label, COUNT(*) AS paper_count
    FROM intermediate.int_topic_clusters tc
    JOIN staging.stg_papers p ON tc.paper_id = p.paper_id
    WHERE p.published_date >= CURRENT_DATE - 30
    GROUP BY topic_label
    ORDER BY paper_count DESC
    LIMIT 20
    """
)

if top_topics:
    df_topics = pd.DataFrame(top_topics)
    fig = px.bar(
        df_topics,
        x="paper_count",
        y="topic_label",
        orientation="h",
        title="Top 20 Research Topics (Last 30 Days)",
        labels={"paper_count": "Papers", "topic_label": "Topic"},
        color="paper_count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No topic data available yet.")

st.divider()

# ── Rising papers feed ────────────────────────────────────────────────────────
st.subheader("🚀 Rising Papers Feed")
st.caption("Papers with citation velocity ≥ 2× their category median (published in last 90 days)")

rising_papers = run_query(
    """
    SELECT
        paper_id, title, topic_label, primary_category,
        published_date, cited_by_count, citation_velocity, velocity_ratio
    FROM marts.mart_rising_papers
    WHERE is_rising = TRUE
    ORDER BY velocity_ratio DESC
    LIMIT 25
    """
)

if rising_papers:
    df_rising = pd.DataFrame(rising_papers)
    df_rising["arxiv_url"] = "https://arxiv.org/abs/" + df_rising["paper_id"]
    df_rising["velocity_ratio"] = df_rising["velocity_ratio"].apply(lambda x: f"{x:.1f}×")
    df_rising["citation_velocity"] = df_rising["citation_velocity"].apply(lambda x: f"{x:.3f}/day")

    st.dataframe(
        df_rising[["title", "primary_category", "published_date",
                   "cited_by_count", "citation_velocity", "velocity_ratio", "arxiv_url"]],
        use_container_width=True,
        column_config={
            "arxiv_url": st.column_config.LinkColumn("arXiv Link"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "velocity_ratio": st.column_config.TextColumn("Velocity Ratio"),
        },
        hide_index=True,
    )
else:
    st.info("No rising papers found. Run the pipeline to ingest more data.")

st.divider()

# ── Category distribution pie ─────────────────────────────────────────────────
st.subheader("Category Distribution")

cat_dist = run_query(
    """
    SELECT primary_category, COUNT(*) AS n
    FROM staging.stg_papers
    WHERE primary_category IS NOT NULL
    GROUP BY primary_category
    ORDER BY n DESC
    LIMIT 15
    """
)

if cat_dist:
    df_cat = pd.DataFrame(cat_dist)
    fig2 = px.pie(
        df_cat,
        values="n",
        names="primary_category",
        title="Papers by arXiv Category",
        hole=0.4,
    )
    fig2.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig2, use_container_width=True)
