/* asset: staging.stg_papers
 *
 * Cleaned, deduplicated, typed paper records joining arXiv metadata
 * with OpenAlex citation enrichment.
 *
 * Business logic:
 *   - Normalize paper_id to strip version suffix (e.g. "2301.00001v2" -> "2301.00001")
 *   - Deduplicate by canonical paper_id (keep latest ingested_at)
 *   - Join OpenAlex citation counts on DOI
 *   - Parse primary category from categories array
 *   - Cast timestamps to consistent UTC
 */

/* @bruin
name: staging.stg_papers
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: merge
  unique_key:
    - paper_id

columns:
  - name: paper_id
    type: text
    description: "Canonical arXiv paper ID without version suffix"
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: text
    description: "Cleaned paper title"
    checks:
      - name: not_null
  - name: abstract
    type: text
    description: "Cleaned abstract"
  - name: author_count
    type: integer
    description: "Number of authors"
    checks:
      - name: positive
  - name: primary_category
    type: text
    description: "Primary arXiv category (first in list)"
  - name: all_categories
    type: text[]
    description: "All arXiv categories as text array"
  - name: published_date
    type: date
    description: "Publication date (UTC)"
  - name: published_year
    type: integer
    description: "Publication year"
  - name: published_week
    type: text
    description: "ISO week string YYYY-Www for trend aggregation"
  - name: doi
    type: text
    description: "DOI if available"
  - name: openalex_work_id
    type: text
    description: "Matched OpenAlex work ID (via DOI)"
  - name: cited_by_count
    type: integer
    description: "Citation count from OpenAlex (0 if not matched)"
  - name: ingested_at
    type: timestamptz
    description: "Landing ingestion timestamp"

depends:
  - landing.raw_arxiv_papers
  - landing.raw_openalex_works
@bruin */

WITH deduped_arxiv AS (
    -- Keep latest version of each paper (strip version from ID)
    SELECT DISTINCT ON (REGEXP_REPLACE(paper_id, 'v\d+$', ''))
        REGEXP_REPLACE(paper_id, 'v\d+$', '')   AS paper_id,
        TRIM(title)                              AS title,
        TRIM(abstract)                           AS abstract,
        -- Extract array elements from JSONB
        JSONB_ARRAY_LENGTH(authors)              AS author_count,
        authors,
        categories,
        published::TIMESTAMPTZ                   AS published_ts,
        doi,
        ingested_at
    FROM landing.raw_arxiv_papers
    WHERE paper_id IS NOT NULL
      AND title IS NOT NULL
      AND title != ''
    ORDER BY
        REGEXP_REPLACE(paper_id, 'v\d+$', ''),
        ingested_at DESC
),

openalex_enrichment AS (
    -- Match on DOI; use LOWER() for case-insensitive comparison
    SELECT
        LOWER(TRIM(doi))  AS doi_normalized,
        work_id           AS openalex_work_id,
        cited_by_count
    FROM landing.raw_openalex_works
    WHERE doi IS NOT NULL AND doi != ''
)

SELECT
    a.paper_id,
    a.title,
    a.abstract,
    COALESCE(a.author_count, 0)                           AS author_count,
    -- Primary category: first element of the JSONB array
    a.categories->>0                                       AS primary_category,
    -- All categories as native text array
    ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(a.categories)) AS all_categories,
    a.published_ts::DATE                                   AS published_date,
    EXTRACT(YEAR FROM a.published_ts)::INT                AS published_year,
    TO_CHAR(a.published_ts, 'IYYY-"W"IW')                AS published_week,
    a.doi,
    oa.openalex_work_id,
    COALESCE(oa.cited_by_count, 0)                        AS cited_by_count,
    a.ingested_at
FROM deduped_arxiv a
LEFT JOIN openalex_enrichment oa
    ON LOWER(TRIM(a.doi)) = oa.doi_normalized
