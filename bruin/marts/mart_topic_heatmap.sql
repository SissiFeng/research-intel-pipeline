/* asset: marts.mart_topic_heatmap
 *
 * Topic co-occurrence matrix: for every pair of arXiv categories/topics
 * that appear together in the same paper, count how often they co-occur.
 *
 * Powers the topic heatmap visualization on the Trends dashboard page.
 * The resulting matrix is (topic_a, topic_b, co_occurrence_count).
 */

/* @bruin
name: marts.mart_topic_heatmap
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: topic_a
    type: text
    description: "First topic (alphabetically)"
    checks:
      - name: not_null
  - name: topic_b
    type: text
    description: "Second topic (alphabetically)"
    checks:
      - name: not_null
  - name: co_occurrence_count
    type: integer
    description: "Number of papers where both topics appear"
    checks:
      - name: positive
  - name: avg_citations
    type: numeric
    description: "Average cited_by_count for papers with both topics"
  - name: paper_sample
    type: text[]
    description: "Up to 3 example paper IDs for drill-down"

depends:
  - staging.stg_papers
  - intermediate.int_topic_clusters
@bruin */

WITH paper_categories AS (
    -- Explode all_categories array to get one row per (paper, category)
    SELECT
        p.paper_id,
        p.cited_by_count,
        UNNEST(p.all_categories) AS category
    FROM staging.stg_papers p
    WHERE p.all_categories IS NOT NULL
      AND ARRAY_LENGTH(p.all_categories, 1) > 1
),

category_pairs AS (
    -- Self-join to get all category co-occurrence pairs per paper
    SELECT
        LEAST(a.category, b.category)     AS topic_a,
        GREATEST(a.category, b.category)  AS topic_b,
        a.paper_id,
        a.cited_by_count
    FROM paper_categories a
    JOIN paper_categories b
        ON a.paper_id = b.paper_id
       AND a.category < b.category        -- avoid duplicates + self-pairs
)

SELECT
    topic_a,
    topic_b,
    COUNT(DISTINCT paper_id)              AS co_occurrence_count,
    ROUND(AVG(cited_by_count), 2)         AS avg_citations,
    -- Sample up to 3 paper IDs for hover drill-down in dashboard
    ARRAY_AGG(DISTINCT paper_id ORDER BY paper_id LIMIT 3) AS paper_sample
FROM category_pairs
GROUP BY topic_a, topic_b
HAVING COUNT(DISTINCT paper_id) >= 2     -- filter noise: at least 2 co-occurrences
ORDER BY co_occurrence_count DESC
