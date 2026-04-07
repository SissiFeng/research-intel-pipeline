"""Dashboard page 4: Author Network — co-authorship graph and influence leaderboard."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Author Network — Research Intel", page_icon="👥", layout="wide")

run_query = st.session_state.get("run_query")
if run_query is None:
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
    from app import run_query  # type: ignore[assignment]

st.title("👥 Author Network")
st.caption("Co-authorship graph and influence leaderboard")

# ── Controls ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Graph Controls")
    max_authors = st.slider("Max authors (nodes)", 20, 150, 60, step=10)
    min_papers = st.slider("Min papers per author", 1, 20, 2, step=1)
    min_collab = st.slider("Min shared papers (edge weight)", 1, 10, 1, step=1)

# ── Influence leaderboard ─────────────────────────────────────────────────────
st.subheader("🏆 Author Influence Leaderboard")

top_authors = run_query(
    f"""
    SELECT
        author_name, paper_count, total_citations,
        h_index_proxy, collaboration_degree, influence_score,
        primary_category
    FROM marts.mart_author_influence
    ORDER BY influence_score DESC
    LIMIT 50
    """
)

if top_authors:
    df_authors = pd.DataFrame(top_authors)
    df_authors["influence_score"] = df_authors["influence_score"].apply(lambda x: f"{x:.2f}")

    st.dataframe(
        df_authors,
        use_container_width=True,
        column_config={
            "author_name": st.column_config.TextColumn("Author", width="medium"),
            "paper_count": st.column_config.NumberColumn("Papers"),
            "total_citations": st.column_config.NumberColumn("Citations"),
            "h_index_proxy": st.column_config.NumberColumn("h-index (proxy)"),
            "collaboration_degree": st.column_config.NumberColumn("Co-authors"),
            "influence_score": st.column_config.TextColumn("Influence Score"),
            "primary_category": st.column_config.TextColumn("Category"),
        },
        hide_index=True,
    )
else:
    st.info("No author influence data. Run the pipeline first.")

st.divider()

# ── h-index distribution ──────────────────────────────────────────────────────
st.subheader("h-index Distribution")

h_dist = run_query(
    """
    SELECT h_index_proxy, COUNT(*) AS n
    FROM marts.mart_author_influence
    WHERE h_index_proxy > 0
    GROUP BY h_index_proxy
    ORDER BY h_index_proxy
    """
)

if h_dist:
    df_h = pd.DataFrame(h_dist)
    fig = px.bar(
        df_h,
        x="h_index_proxy",
        y="n",
        title="h-index Distribution Across Authors",
        labels={"h_index_proxy": "h-index (proxy)", "n": "Authors"},
        color="n",
        color_continuous_scale="Teal",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Co-authorship network graph ───────────────────────────────────────────────
st.subheader("Co-authorship Network")

network_edges = run_query(
    f"""
    SELECT author_a, author_b, shared_paper_count
    FROM intermediate.int_author_network
    WHERE shared_paper_count >= {min_collab}
    ORDER BY shared_paper_count DESC
    LIMIT {max_authors * 5}
    """
)

if not network_edges:
    st.info("No co-authorship data. Run the full pipeline with real papers.")
    st.stop()

df_net = pd.DataFrame(network_edges)

# Filter to top authors by degree
author_degree = (
    pd.concat([
        df_net.groupby("author_a")["shared_paper_count"].sum().rename("degree"),
        df_net.groupby("author_b")["shared_paper_count"].sum().rename("degree"),
    ])
    .groupby(level=0)
    .sum()
    .nlargest(max_authors)
)
top_author_set = set(author_degree.index.tolist())
df_net_filtered = df_net[
    df_net["author_a"].isin(top_author_set) & df_net["author_b"].isin(top_author_set)
]

try:
    from pyvis.network import Network
except ImportError:
    st.error("pyvis is not installed. Run: pip install pyvis")
    st.stop()

net = Network(
    height="600px",
    width="100%",
    bgcolor="#0e1117",
    font_color="#ffffff",
    directed=False,
)
net.set_options(
    """
    {
      "physics": {
        "solver": "forceAtlas2Based",
        "stabilization": {"iterations": 150}
      },
      "edges": {
        "color": {"color": "#2ecc71", "opacity": 0.5},
        "smooth": {"enabled": true}
      },
      "nodes": {
        "shape": "dot",
        "font": {"size": 9}
      }
    }
    """
)

# Fetch category color for top authors
author_cats = run_query(
    """
    SELECT author_name_lower, primary_category
    FROM staging.stg_authors
    """
)
cat_map = {r["author_name_lower"]: r["primary_category"] for r in (author_cats or [])}

CAT_COLORS: dict[str, str] = {
    "cs.AI": "#e74c3c", "cs.LG": "#3498db", "cs.CV": "#2ecc71",
    "cs.NLP": "#f39c12", "cs.CL": "#f39c12", "q-bio": "#9b59b6",
}

for author in top_author_set:
    degree = int(author_degree.get(author, 1))
    cat = cat_map.get(author.lower(), "other")
    color = CAT_COLORS.get(cat, "#95a5a6")
    size = max(6, min(30, 4 + degree))
    net.add_node(
        author,
        label=author[:20],
        title=f"<b>{author}</b><br>Degree: {degree}<br>Category: {cat}",
        color=color,
        size=size,
    )

for _, row in df_net_filtered.iterrows():
    net.add_edge(
        row["author_a"],
        row["author_b"],
        value=int(row["shared_paper_count"]),
        title=f"Shared papers: {row['shared_paper_count']}",
    )

html_path = "/tmp/author_network.html"
net.save_graph(html_path)
with open(html_path, "r") as f:
    html_content = f.read()

col_graph, col_info = st.columns([3, 1])

with col_graph:
    components.html(html_content, height=620, scrolling=False)

with col_info:
    st.subheader("Stats")
    st.metric("Authors (nodes)", len(top_author_set))
    st.metric("Collaborations (edges)", len(df_net_filtered))

    st.subheader("Category Legend")
    for cat, color in CAT_COLORS.items():
        st.markdown(
            f'<span style="color:{color}">●</span> {cat}',
            unsafe_allow_html=True,
        )
