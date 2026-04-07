/* asset: marts.mart_research_trends
 *
 * Weekly paper volume and citation velocity by topic/category.
 * Primary table for the Trends dashboard page.
 *
 * Metrics:
 *   - paper_count: new papers published that week
 *   - total_citations: sum of cited_by_count for papers published that week
 *   - avg_citations: average citations per paper that week
 *   - citation_velocity: ratio of citations to paper_count (engagement signal)
 *   - cumulative_papers: rolling total paper count per topic
 */

/* @bruin
name: marts.mart_research_trends
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: topic_label
    type: text
    description: "Topic cluster label"
    checks:
      - name: not_null
  - name: published_week
    type: text
    description: "ISO week string YYYY-Www"
    checks:
      - name: not_null
  - name: published_year
    type: integer
    description: "Year extracted from published_week"
  - name: paper_count
    type: integer
    description: "Number of papers published in this week"
    checks:
      - name: positive
  - name: total_citations
    type: integer
    description: "Sum of cited_by_count for papers published this week"
    checks:
      - name: not_negative
  - name: avg_citations
    type: numeric
    description: "Average citations per paper"
  - name: citation_velocity
    type: numeric
    description: "Citations per paper: proxy for engagement/impact"
  - name: cumulative_papers
    type: bigint
    description: "Running total papers per topic up to this week"

depends:
  - intermediate.int_topic_clusters
  - staging.stg_papers
@bruin */

WITH weekly_stats AS (
    SELECT
        tc.topic_label,
        p.published_week,
        p.published_year,
        COUNT(*)                            AS paper_count,
        SUM(p.cited_by_count)               AS total_citations,
        ROUND(AVG(p.cited_by_count), 2)     AS avg_citations
    FROM intermediate.int_topic_clusters tc
    JOIN staging.stg_papers p
        ON tc.paper_id = p.paper_id
    WHERE p.published_week IS NOT NULL
    GROUP BY tc.topic_label, p.published_week, p.published_year
)

SELECT
    topic_label,
    published_week,
    published_year,
    paper_count,
    total_citations,
    avg_citations,
    ROUND(
        CASE WHEN paper_count > 0
             THEN total_citations::NUMERIC / paper_count
             ELSE 0
        END,
        3
    )                                       AS citation_velocity,
    SUM(paper_count) OVER (
        PARTITION BY topic_label
        ORDER BY published_week
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                       AS cumulative_papers
FROM weekly_stats
ORDER BY published_week DESC, paper_count DESC
