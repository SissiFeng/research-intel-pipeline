/* asset: marts.mart_rising_papers
 *
 * Papers with citation growth > 2x compared to 30 days ago.
 * This is the "rising stars" feed shown on the Overview dashboard.
 *
 * Citation growth proxy:
 *   We don't have historical snapshots, so we use citation_velocity
 *   (citations per day since publication) as the rising signal.
 *   Papers published within the last 90 days with high velocity
 *   are considered "rising".
 *
 * A paper is "rising" if:
 *   cited_by_count / days_since_published >= 2x the median
 *   velocity for papers in the same category and cohort.
 */

/* @bruin
name: marts.mart_rising_papers
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
  - name: title
    type: text
    description: "Paper title"
    checks:
      - name: not_null
  - name: topic_label
    type: text
    description: "Assigned topic cluster"
  - name: primary_category
    type: text
    description: "arXiv primary category"
  - name: published_date
    type: date
    description: "Publication date"
  - name: days_since_published
    type: integer
    description: "Days elapsed since publication"
  - name: cited_by_count
    type: integer
    description: "Total citations from OpenAlex"
  - name: citation_velocity
    type: numeric
    description: "Citations per day since publication"
  - name: category_median_velocity
    type: numeric
    description: "Median citation velocity for same category + year cohort"
  - name: velocity_ratio
    type: numeric
    description: "citation_velocity / category_median_velocity"
  - name: is_rising
    type: boolean
    description: "True if velocity_ratio >= 2.0"
  - name: author_count
    type: integer
    description: "Number of authors"

depends:
  - staging.stg_papers
  - intermediate.int_topic_clusters
@bruin */

WITH paper_velocity AS (
    SELECT
        p.paper_id,
        p.title,
        tc.topic_label,
        p.primary_category,
        p.published_date,
        p.cited_by_count,
        p.author_count,
        GREATEST(
            (CURRENT_DATE - p.published_date),
            1
        )                                                         AS days_since_published,
        -- Citation velocity: citations per day
        ROUND(
            p.cited_by_count::NUMERIC
            / GREATEST((CURRENT_DATE - p.published_date), 1),
            4
        )                                                         AS citation_velocity
    FROM staging.stg_papers p
    JOIN intermediate.int_topic_clusters tc
        ON p.paper_id = tc.paper_id
    -- Focus on recent papers (last 90 days) for "rising" detection
    WHERE p.published_date >= CURRENT_DATE - INTERVAL '90 days'
      AND p.published_date IS NOT NULL
),

category_benchmarks AS (
    -- Compute median velocity per category + year cohort
    SELECT
        primary_category,
        EXTRACT(YEAR FROM published_date)::INT          AS cohort_year,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY citation_velocity
        )                                               AS median_velocity
    FROM paper_velocity
    GROUP BY primary_category, EXTRACT(YEAR FROM published_date)::INT
)

SELECT
    pv.paper_id,
    pv.title,
    pv.topic_label,
    pv.primary_category,
    pv.published_date,
    pv.days_since_published,
    pv.cited_by_count,
    pv.citation_velocity,
    COALESCE(cb.median_velocity, 0)                AS category_median_velocity,
    ROUND(
        CASE
            WHEN COALESCE(cb.median_velocity, 0) > 0
            THEN pv.citation_velocity / cb.median_velocity
            ELSE 0
        END,
        2
    )                                              AS velocity_ratio,
    CASE
        WHEN COALESCE(cb.median_velocity, 0) > 0
             AND pv.citation_velocity / cb.median_velocity >= 2.0
        THEN TRUE
        ELSE FALSE
    END                                            AS is_rising,
    pv.author_count
FROM paper_velocity pv
LEFT JOIN category_benchmarks cb
    ON pv.primary_category = cb.primary_category
   AND EXTRACT(YEAR FROM pv.published_date)::INT = cb.cohort_year
ORDER BY velocity_ratio DESC NULLS LAST
