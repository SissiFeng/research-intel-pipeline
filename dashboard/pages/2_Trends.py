"""Dashboard page 2: Trends — topic volume over time, citation velocity heatmap."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Trends — Research Intel", page_icon="📈", layout="wide")

run_query = st.session_state.get("run_query")
if run_query is None:
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
    from app import run_query  # type: ignore[assignment]

st.title("📈 Research Trends")
st.caption("Topic volume and citation velocity over time")

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    categories_raw = run_query(
        "SELECT DISTINCT primary_category FROM staging.stg_papers ORDER BY primary_category"
    )
    all_cats = [r["primary_category"] for r in categories_raw if r["primary_category"]]

    selected_cats = st.multiselect(
        "Filter by Category",
        options=all_cats,
        default=all_cats[:5] if len(all_cats) >= 5 else all_cats,
    )

    weeks_back = st.slider("Weeks to show", min_value=4, max_value=52, value=24, step=4)

# ── Topic volume over time ────────────────────────────────────────────────────
st.subheader("Paper Volume by Topic per Week")

trend_data = run_query(
    f"""
    SELECT topic_label, published_week, paper_count, citation_velocity
    FROM marts.mart_research_trends
    WHERE published_week >= TO_CHAR(
        (CURRENT_DATE - INTERVAL '{weeks_back} weeks')::DATE,
        'IYYY-"W"IW'
    )
    ORDER BY published_week ASC
    """
)

if trend_data:
    df_trends = pd.DataFrame(trend_data)

    # Filter by selected categories if we have topic-category mapping
    if selected_cats:
        topic_cat_map = run_query(
            """
            SELECT DISTINCT tc.topic_label, p.primary_category
            FROM intermediate.int_topic_clusters tc
            JOIN staging.stg_papers p ON tc.paper_id = p.paper_id
            """
        )
        if topic_cat_map:
            df_map = pd.DataFrame(topic_cat_map)
            valid_topics = df_map[df_map["primary_category"].isin(selected_cats)]["topic_label"].unique()
            df_trends = df_trends[df_trends["topic_label"].isin(valid_topics)]

    # Top N topics by total volume for legibility
    top_topics = (
        df_trends.groupby("topic_label")["paper_count"]
        .sum()
        .nlargest(10)
        .index.tolist()
    )
    df_plot = df_trends[df_trends["topic_label"].isin(top_topics)]

    fig = px.line(
        df_plot,
        x="published_week",
        y="paper_count",
        color="topic_label",
        title=f"Top 10 Topics — Weekly Paper Volume (last {weeks_back} weeks)",
        labels={"published_week": "Week", "paper_count": "Papers", "topic_label": "Topic"},
        markers=True,
    )
    fig.update_layout(xaxis_tickangle=-45, legend_title_text="Topic")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No trend data available. Run the pipeline to populate mart_research_trends.")

st.divider()

# ── Citation velocity heatmap ─────────────────────────────────────────────────
st.subheader("Citation Velocity Heatmap (Topic × Week)")
st.caption("Color = average citations per paper for that topic–week cell")

heatmap_data = run_query(
    f"""
    SELECT topic_label, published_week, ROUND(AVG(citation_velocity), 3) AS avg_velocity
    FROM marts.mart_research_trends
    WHERE published_week >= TO_CHAR(
        (CURRENT_DATE - INTERVAL '{weeks_back} weeks')::DATE,
        'IYYY-"W"IW'
    )
    GROUP BY topic_label, published_week
    ORDER BY topic_label, published_week
    """
)

if heatmap_data:
    df_heat = pd.DataFrame(heatmap_data)
    # Pivot for heatmap
    df_pivot = df_heat.pivot(index="topic_label", columns="published_week", values="avg_velocity")
    # Keep top 15 topics by mean velocity
    df_pivot = df_pivot.loc[
        df_pivot.mean(axis=1).nlargest(15).index
    ]

    fig2 = go.Figure(
        data=go.Heatmap(
            z=df_pivot.values.tolist(),
            x=df_pivot.columns.tolist(),
            y=df_pivot.index.tolist(),
            colorscale="Viridis",
            colorbar_title="Citations/Paper",
            hoverongaps=False,
        )
    )
    fig2.update_layout(
        title="Citation Velocity Heatmap",
        xaxis_title="Week",
        yaxis_title="Topic",
        height=500,
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No heatmap data available.")

st.divider()

# ── Topic co-occurrence heatmap ───────────────────────────────────────────────
st.subheader("Topic Co-occurrence Matrix")
st.caption("How often two arXiv categories appear together in the same paper")

cooc_data = run_query(
    """
    SELECT topic_a, topic_b, co_occurrence_count
    FROM marts.mart_topic_heatmap
    ORDER BY co_occurrence_count DESC
    LIMIT 200
    """
)

if cooc_data:
    df_cooc = pd.DataFrame(cooc_data)
    all_topics = sorted(
        set(df_cooc["topic_a"].tolist() + df_cooc["topic_b"].tolist())
    )[:20]  # limit to top 20 for readability

    # Build symmetric matrix
    import numpy as np
    mat = pd.DataFrame(0, index=all_topics, columns=all_topics)
    for _, row in df_cooc.iterrows():
        a, b = row["topic_a"], row["topic_b"]
        if a in mat.index and b in mat.columns:
            mat.loc[a, b] = row["co_occurrence_count"]
            mat.loc[b, a] = row["co_occurrence_count"]

    fig3 = px.imshow(
        mat,
        title="Topic Co-occurrence (top 20 categories)",
        color_continuous_scale="Blues",
        aspect="auto",
    )
    fig3.update_layout(height=600)
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No co-occurrence data available.")
