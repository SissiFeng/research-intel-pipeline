/* asset: staging.stg_citations
 *
 * Citation edge table: one row per (citing_work, cited_work) pair.
 * Sources citation edges from OpenAlex referenced_works arrays.
 * These edges form the citation graph used in mart_research_trends
 * and the Citation Network dashboard page.
 */

/* @bruin
name: staging.stg_citations
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: replace

columns:
  - name: citing_work_id
    type: text
    description: "OpenAlex work ID of the citing paper"
    checks:
      - name: not_null
  - name: cited_work_id
    type: text
    description: "OpenAlex work ID of the cited paper"
    checks:
      - name: not_null
  - name: citing_doi
    type: text
    description: "DOI of citing paper (for arXiv joins)"
  - name: cited_doi
    type: text
    description: "DOI of cited paper (for arXiv joins)"
  - name: citing_paper_id
    type: text
    description: "arXiv paper ID of citing paper if matched"
  - name: cited_paper_id
    type: text
    description: "arXiv paper ID of cited paper if matched"
  - name: citing_year
    type: integer
    description: "Publication year of citing paper"

depends:
  - landing.raw_openalex_works
  - staging.stg_papers
@bruin */

WITH citation_edges AS (
    -- Explode referenced_works array to get (citing -> cited) edges
    SELECT
        oa.work_id                                        AS citing_work_id,
        LOWER(TRIM(oa.doi))                               AS citing_doi,
        oa.publication_year                               AS citing_year,
        TRIM(ref.value::TEXT, '"')                        AS cited_work_id
    FROM landing.raw_openalex_works oa,
         JSONB_ARRAY_ELEMENTS(oa.referenced_works) AS ref(value)
    WHERE oa.work_id IS NOT NULL
      AND TRIM(ref.value::TEXT, '"') != ''
),

doi_to_paper AS (
    -- Lookup table: DOI -> canonical arXiv paper_id
    SELECT
        LOWER(TRIM(doi))  AS doi_normalized,
        paper_id
    FROM staging.stg_papers
    WHERE doi IS NOT NULL AND doi != ''
),

cited_work_doi AS (
    -- Resolve cited_work_id back to DOI for arXiv matching
    SELECT
        work_id,
        LOWER(TRIM(doi)) AS doi_normalized
    FROM landing.raw_openalex_works
    WHERE doi IS NOT NULL
)

SELECT
    ce.citing_work_id,
    ce.cited_work_id,
    ce.citing_doi,
    cwd.doi_normalized                     AS cited_doi,
    -- Resolve to arXiv IDs where possible
    citing_map.paper_id                    AS citing_paper_id,
    cited_map.paper_id                     AS cited_paper_id,
    ce.citing_year
FROM citation_edges ce
LEFT JOIN cited_work_doi cwd
    ON ce.cited_work_id = cwd.work_id
LEFT JOIN doi_to_paper citing_map
    ON ce.citing_doi = citing_map.doi_normalized
LEFT JOIN doi_to_paper cited_map
    ON cwd.doi_normalized = cited_map.doi_normalized
