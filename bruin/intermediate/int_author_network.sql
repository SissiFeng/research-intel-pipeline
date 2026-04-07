/* asset: intermediate.int_author_network
 *
 * Co-authorship edge table: one row per unique (author_a, author_b) pair
 * where both authors share at least one paper.
 * author_a < author_b (alphabetical) to avoid duplicate undirected edges.
 *
 * Used by: mart_author_influence, dashboard Author Network page.
 */

/* @bruin
name: intermediate.int_author_network
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: author_a
    type: text
    description: "Author name (alphabetically first)"
    checks:
      - name: not_null
  - name: author_b
    type: text
    description: "Author name (alphabetically second)"
    checks:
      - name: not_null
  - name: shared_paper_count
    type: integer
    description: "Number of papers co-authored"
    checks:
      - name: positive
  - name: first_collaboration
    type: date
    description: "Date of first co-authored paper"
  - name: last_collaboration
    type: date
    description: "Date of most recent co-authored paper"
  - name: primary_category
    type: text
    description: "Most common category among shared papers"

depends:
  - staging.stg_papers
  - landing.raw_arxiv_papers
@bruin */

WITH author_paper AS (
    -- Flatten authors from landing; join to canonical paper IDs + dates
    SELECT
        REGEXP_REPLACE(r.paper_id, 'v\d+$', '')              AS paper_id,
        LOWER(TRIM(author_name.value::TEXT, '"'))             AS author_lower,
        TRIM(author_name.value::TEXT, '"')                    AS author_display,
        r.published::DATE                                      AS published_date,
        r.categories->>0                                       AS primary_category
    FROM landing.raw_arxiv_papers r,
         JSONB_ARRAY_ELEMENTS(r.authors) AS author_name(value)
    WHERE r.paper_id IS NOT NULL
      AND TRIM(author_name.value::TEXT, '"') != ''
),

-- Self-join to get all co-author pairs per paper
co_author_pairs AS (
    SELECT
        LEAST(a.author_lower, b.author_lower)        AS author_a_lower,
        GREATEST(a.author_lower, b.author_lower)     AS author_b_lower,
        LEAST(a.author_display, b.author_display)    AS author_a,
        GREATEST(a.author_display, b.author_display) AS author_b,
        a.paper_id,
        a.published_date,
        a.primary_category
    FROM author_paper a
    JOIN author_paper b
        ON a.paper_id = b.paper_id
       AND a.author_lower < b.author_lower
)

SELECT
    author_a,
    author_b,
    COUNT(DISTINCT paper_id)                         AS shared_paper_count,
    MIN(published_date)                              AS first_collaboration,
    MAX(published_date)                              AS last_collaboration,
    MODE() WITHIN GROUP (ORDER BY primary_category)  AS primary_category
FROM co_author_pairs
GROUP BY author_a, author_b
HAVING COUNT(DISTINCT paper_id) >= 1
