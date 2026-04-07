/* asset: intermediate.int_topic_clusters
 *
 * Assigns a topic cluster label to each paper using:
 *  1. TF-IDF top keyword from int_paper_embeddings (primary)
 *  2. arXiv primary_category as fallback
 *
 * Also computes topic co-occurrence: which pairs of topics appear
 * in the same paper (via categories), powering the topic heatmap.
 *
 * Writes two outputs:
 *   - int_topic_clusters          (paper -> topic assignment)
 *   - int_topic_cooccurrence      (topic_a, topic_b, co-occurrence count)
 */

/* @bruin
name: intermediate.int_topic_clusters
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: paper_id
    type: text
    description: "Canonical arXiv paper ID"
    checks:
      - name: not_null
      - name: unique
  - name: topic_label
    type: text
    description: "Assigned topic label (TF-IDF keyword or arXiv category)"
    checks:
      - name: not_null
  - name: topic_source
    type: text
    description: "'tfidf' or 'category' indicating assignment source"
  - name: primary_category
    type: text
    description: "arXiv primary category"
  - name: published_date
    type: date
    description: "Paper publication date for time-series analysis"
  - name: cited_by_count
    type: integer
    description: "Citation count for weighted topic analysis"

depends:
  - staging.stg_papers
  - intermediate.int_paper_embeddings
@bruin */

SELECT
    p.paper_id,
    COALESCE(
        NULLIF(e.topic_label, 'unknown'),   -- prefer TF-IDF label
        p.primary_category,                  -- fallback to arXiv category
        'other'
    )                                         AS topic_label,
    CASE
        WHEN e.topic_label IS NOT NULL AND e.topic_label != 'unknown' THEN 'tfidf'
        ELSE 'category'
    END                                       AS topic_source,
    p.primary_category,
    p.published_date,
    p.cited_by_count
FROM staging.stg_papers p
LEFT JOIN intermediate.int_paper_embeddings e
    ON p.paper_id = e.paper_id
