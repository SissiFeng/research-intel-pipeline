/* asset: staging.stg_authors
 *
 * Normalized author entity table.
 * Explodes the JSONB authors array from raw_arxiv_papers into one row per author,
 * enriched with institution data from OpenAlex authorships.
 *
 * Deduplication: authors are identified by normalized name (lowercased, trimmed).
 * In production, entity resolution would use fuzzy matching; here we use exact name.
 */

/* @bruin
name: staging.stg_authors
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: author_name
    type: text
    description: "Normalized author name (trimmed)"
    checks:
      - name: not_null
  - name: author_name_lower
    type: text
    description: "Lowercase author name for deduplication joins"
    checks:
      - name: not_null
      - name: unique
  - name: paper_count
    type: integer
    description: "Number of papers this author appears in"
    checks:
      - name: positive
  - name: first_paper_date
    type: date
    description: "Date of earliest paper in the dataset"
  - name: last_paper_date
    type: date
    description: "Date of most recent paper in the dataset"
  - name: primary_category
    type: text
    description: "Most frequent arXiv category across this author's papers"
  - name: openalex_author_id
    type: text
    description: "OpenAlex author ID if matched via authorships"

depends:
  - staging.stg_papers
  - landing.raw_arxiv_papers
  - landing.raw_openalex_works
@bruin */

WITH author_paper_edges AS (
    -- Explode authors JSONB array from landing table, join to staged paper IDs
    SELECT
        REGEXP_REPLACE(r.paper_id, 'v\d+$', '')  AS paper_id,
        TRIM(author_name.value::TEXT, '"')         AS author_name,
        r.published::DATE                          AS published_date,
        r.categories->>0                           AS primary_category
    FROM landing.raw_arxiv_papers r,
         JSONB_ARRAY_ELEMENTS(r.authors) AS author_name(value)
    WHERE r.paper_id IS NOT NULL
      AND TRIM(author_name.value::TEXT, '"') != ''
),

openalex_authors AS (
    -- Flatten OpenAlex authorship records for ID enrichment
    SELECT DISTINCT
        LOWER(TRIM(auth.value->'author'->>'display_name'))  AS author_name_lower,
        auth.value->'author'->>'id'                         AS openalex_author_id
    FROM landing.raw_openalex_works oa,
         JSONB_ARRAY_ELEMENTS(oa.authorships) AS auth(value)
    WHERE auth.value->'author'->>'display_name' IS NOT NULL
)

SELECT
    -- Use the most common non-empty casing for display
    MODE() WITHIN GROUP (ORDER BY ape.author_name)  AS author_name,
    LOWER(ape.author_name)                           AS author_name_lower,
    COUNT(DISTINCT ape.paper_id)                     AS paper_count,
    MIN(ape.published_date)                          AS first_paper_date,
    MAX(ape.published_date)                          AS last_paper_date,
    MODE() WITHIN GROUP (ORDER BY ape.primary_category) AS primary_category,
    MAX(oa.openalex_author_id)                       AS openalex_author_id
FROM author_paper_edges ape
LEFT JOIN openalex_authors oa
    ON LOWER(ape.author_name) = oa.author_name_lower
GROUP BY LOWER(ape.author_name)
HAVING COUNT(DISTINCT ape.paper_id) > 0
