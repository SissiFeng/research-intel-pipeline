"""Dashboard page 3: Citation Network — interactive paper-paper citation graph."""

from __future__ import annotations

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Citation Network — Research Intel", page_icon="🕸️", layout="wide")

run_query = st.session_state.get("run_query")
if run_query is None:
    import sys
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parents[1]))
    from app import run_query  # type: ignore[assignment]

st.title("🕸️ Citation Network")
st.caption("Interactive paper-to-paper citation graph powered by pyvis")

# ── Controls ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Graph Controls")
    max_nodes = st.slider("Max papers (nodes)", 20, 200, 60, step=10)
    min_citations = st.slider("Min citations for inclusion", 0, 50, 5, step=5)
    category_filter = st.text_input("Filter by category (e.g. cs.AI)", value="")
    layout_algo = st.selectbox(
        "Layout algorithm",
        options=["forceAtlas2Based", "barnesHut", "repulsion"],
        index=0,
    )

# ── Fetch citation edges ──────────────────────────────────────────────────────
cat_clause = ""
if category_filter.strip():
    cat_clause = f"AND (p1.primary_category ILIKE '%{category_filter.strip()}%' OR p2.primary_category ILIKE '%{category_filter.strip()}%')"

edge_query = f"""
SELECT
    sc.citing_paper_id   AS source,
    sc.cited_paper_id    AS target,
    p1.title             AS source_title,
    p2.title             AS target_title,
    p1.cited_by_count    AS source_citations,
    p2.cited_by_count    AS target_citations,
    p1.primary_category  AS source_category
FROM staging.stg_citations sc
JOIN staging.stg_papers p1 ON sc.citing_paper_id = p1.paper_id
JOIN staging.stg_papers p2 ON sc.cited_paper_id  = p2.paper_id
WHERE sc.citing_paper_id IS NOT NULL
  AND sc.cited_paper_id  IS NOT NULL
  AND p1.cited_by_count >= {min_citations}
  {cat_clause}
LIMIT {max_nodes * 3}
"""

edges = run_query(edge_query)

if not edges:
    st.info(
        "No citation edges available. This requires papers matched between arXiv and OpenAlex via DOI. "
        "Run the full pipeline with real data to populate this view."
    )
    st.stop()

df_edges = pd.DataFrame(edges)

# ── Build pyvis graph ─────────────────────────────────────────────────────────
try:
    from pyvis.network import Network
except ImportError:
    st.error("pyvis is not installed. Run: pip install pyvis")
    st.stop()

# Collect unique nodes (limit to max_nodes)
all_node_ids = list(
    set(df_edges["source"].tolist() + df_edges["target"].tolist())
)[:max_nodes]
df_edges_filtered = df_edges[
    df_edges["source"].isin(all_node_ids) & df_edges["target"].isin(all_node_ids)
]

net = Network(
    height="650px",
    width="100%",
    bgcolor="#0e1117",
    font_color="#ffffff",
    directed=True,
)
net.set_options(
    f"""
    {{
      "physics": {{
        "solver": "{layout_algo}",
        "stabilization": {{"iterations": 100}}
      }},
      "edges": {{
        "arrows": {{"to": {{"enabled": true, "scaleFactor": 0.5}}}},
        "color": {{"color": "#4a9eff", "opacity": 0.6}},
        "smooth": {{"enabled": true, "type": "curvedCW", "roundness": 0.2}}
      }},
      "nodes": {{
        "shape": "dot",
        "font": {{"size": 10}}
      }}
    }}
    """
)

# Category color mapping
CATEGORY_COLORS: dict[str, str] = {
    "cs.AI": "#e74c3c",
    "cs.LG": "#3498db",
    "cs.CV": "#2ecc71",
    "cs.NLP": "#f39c12",
    "cs.CL": "#f39c12",
    "q-bio": "#9b59b6",
    "stat.ML": "#1abc9c",
}

node_info: dict[str, dict] = {}
for _, row in df_edges.iterrows():
    for pid, title, cat, cit in [
        (row["source"], row["source_title"], row["source_category"], row["source_citations"]),
        (row["target"], row["target_title"], row["source_category"], row["target_citations"]),
    ]:
        if pid not in node_info:
            node_info[pid] = {"title": title, "category": cat, "citations": cit}

for pid in all_node_ids:
    info = node_info.get(pid, {})
    title = info.get("title", pid)
    cat = info.get("category", "other")
    cit = info.get("citations", 0)
    color = CATEGORY_COLORS.get(cat, "#95a5a6")
    size = max(5, min(30, 5 + cit // 5))
    short_title = title[:40] + "…" if len(title) > 40 else title
    net.add_node(
        pid,
        label=short_title,
        title=f"<b>{title}</b><br>Category: {cat}<br>Citations: {cit}<br>ID: {pid}",
        color=color,
        size=size,
    )

for _, row in df_edges_filtered.iterrows():
    if row["source"] in all_node_ids and row["target"] in all_node_ids:
        net.add_edge(row["source"], row["target"])

html_path = "/tmp/citation_network.html"
net.save_graph(html_path)
with open(html_path, "r") as f:
    html_content = f.read()

# ── Render ────────────────────────────────────────────────────────────────────
col_graph, col_stats = st.columns([3, 1])

with col_graph:
    components.html(html_content, height=670, scrolling=False)

with col_stats:
    st.subheader("Graph Stats")
    st.metric("Nodes (papers)", len(all_node_ids))
    st.metric("Edges (citations)", len(df_edges_filtered))

    st.subheader("Top Cited Papers")
    top_cited = run_query(
        f"""
        SELECT paper_id, title, cited_by_count, primary_category
        FROM staging.stg_papers
        WHERE paper_id = ANY(ARRAY[{','.join(f"'{p}'" for p in all_node_ids[:50])}]::TEXT[])
        ORDER BY cited_by_count DESC
        LIMIT 10
        """
    )
    if top_cited:
        for r in top_cited:
            with st.expander(f"📄 {r['title'][:45]}…"):
                st.write(f"**Category:** {r['primary_category']}")
                st.write(f"**Citations:** {r['cited_by_count']}")
                st.write(f"[arXiv →](https://arxiv.org/abs/{r['paper_id']})")
