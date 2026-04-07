/* asset: marts.mart_author_influence
 *
 * Author influence scoring with h-index proxy and collaboration degree.
 *
 * h-index proxy: largest h such that author has at least h papers
 *                each cited at least h times.
 *
 * Collaboration degree: number of unique co-authors (network degree).
 */

/* @bruin
name: marts.mart_author_influence
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: author_name
    type: text
    description: "Author display name"
    checks:
      - name: not_null
      - name: unique
  - name: paper_count
    type: integer
    description: "Total papers in dataset"
    checks:
      - name: positive
  - name: total_citations
    type: integer
    description: "Sum of all citations across author's papers"
    checks:
      - name: not_negative
  - name: h_index_proxy
    type: integer
    description: "h-index computed from available citation data"
  - name: collaboration_degree
    type: integer
    description: "Number of unique co-authors"
  - name: primary_category
    type: text
    description: "Author's most frequent arXiv category"
  - name: first_paper_date
    type: date
    description: "Date of earliest paper"
  - name: last_paper_date
    type: date
    description: "Date of most recent paper"
  - name: active_years
    type: integer
    description: "Years between first and last paper"
  - name: influence_score
    type: numeric
    description: "Composite score: h_index * log(1 + total_citations) * log(1 + paper_count)"

depends:
  - staging.stg_authors
  - staging.stg_papers
  - intermediate.int_author_network
  - landing.raw_arxiv_papers
@bruin */

WITH author_paper_citations AS (
    -- Each author's paper with its citation count
    SELECT
        LOWER(TRIM(author_name.value::TEXT, '"'))     AS author_lower,
        TRIM(author_name.value::TEXT, '"')             AS author_display,
        REGEXP_REPLACE(r.paper_id, 'v\d+$', '')       AS paper_id,
        COALESCE(p.cited_by_count, 0)                  AS cited_by_count
    FROM landing.raw_arxiv_papers r
    JOIN staging.stg_papers p
        ON REGEXP_REPLACE(r.paper_id, 'v\d+$', '') = p.paper_id,
         JSONB_ARRAY_ELEMENTS(r.authors) AS author_name(value)
    WHERE TRIM(author_name.value::TEXT, '"') != ''
),

h_index_calc AS (
    -- Compute h-index per author using window rank trick
    SELECT
        author_lower,
        MAX(h_val) AS h_index_proxy
    FROM (
        SELECT
            author_lower,
            cited_by_count,
            ROW_NUMBER() OVER (
                PARTITION BY author_lower
                ORDER BY cited_by_count DESC
            ) AS rank_desc,
            CASE
                WHEN cited_by_count >= ROW_NUMBER() OVER (
                    PARTITION BY author_lower
                    ORDER BY cited_by_count DESC
                )
                THEN ROW_NUMBER() OVER (
                    PARTITION BY author_lower
                    ORDER BY cited_by_count DESC
                )
                ELSE 0
            END AS h_val
        FROM author_paper_citations
    ) ranked
    GROUP BY author_lower
),

author_stats AS (
    SELECT
        author_lower,
        MODE() WITHIN GROUP (ORDER BY author_display) AS author_name,
        COUNT(DISTINCT paper_id)                        AS paper_count,
        SUM(cited_by_count)                             AS total_citations
    FROM author_paper_citations
    GROUP BY author_lower
),

collaboration_degree AS (
    -- Count unique co-authors per author from the co-authorship network
    SELECT
        LOWER(author_a) AS author_lower,
        COUNT(DISTINCT LOWER(author_b)) AS collab_degree
    FROM intermediate.int_author_network
    GROUP BY LOWER(author_a)
    UNION ALL
    SELECT
        LOWER(author_b) AS author_lower,
        COUNT(DISTINCT LOWER(author_a)) AS collab_degree
    FROM intermediate.int_author_network
    GROUP BY LOWER(author_b)
)

SELECT
    ast.author_name,
    ast.paper_count,
    ast.total_citations,
    COALESCE(hi.h_index_proxy, 0)                              AS h_index_proxy,
    COALESCE(SUM(cd.collab_degree), 0)::INT                   AS collaboration_degree,
    sa.primary_category,
    sa.first_paper_date,
    sa.last_paper_date,
    EXTRACT(YEAR FROM sa.last_paper_date)::INT
        - EXTRACT(YEAR FROM sa.first_paper_date)::INT          AS active_years,
    ROUND(
        COALESCE(hi.h_index_proxy, 0)
        * LN(1 + ast.total_citations)
        * LN(1 + ast.paper_count),
        3
    )                                                          AS influence_score
FROM author_stats ast
LEFT JOIN h_index_calc hi
    ON ast.author_lower = hi.author_lower
LEFT JOIN staging.stg_authors sa
    ON ast.author_lower = sa.author_name_lower
LEFT JOIN collaboration_degree cd
    ON ast.author_lower = cd.author_lower
GROUP BY
    ast.author_name, ast.paper_count, ast.total_citations,
    hi.h_index_proxy, sa.primary_category, sa.first_paper_date,
    sa.last_paper_date
ORDER BY influence_score DESC NULLS LAST
