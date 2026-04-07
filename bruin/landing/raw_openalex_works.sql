/* asset: landing.raw_openalex_works
 *
 * Landing table for raw OpenAlex work records ingested via Kafka consumer.
 * Contains citation counts, concepts, and authorship data matched to arXiv papers.
 *
 * Upstream: Redpanda topic raw_openalex_works (via openalex_consumer.py)
 * Downstream: staging.stg_papers (citation enrichment), staging.stg_citations
 */

/* @bruin
name: landing.raw_openalex_works
type: postgres.sql
connection: supabase_pg

materialization:
  type: table
  strategy: merge
  unique_key:
    - work_id

columns:
  - name: work_id
    type: text
    description: "OpenAlex work identifier (https://openalex.org/W...)"
    checks:
      - name: not_null
      - name: unique
  - name: doi
    type: text
    description: "DOI without https://doi.org/ prefix"
  - name: title
    type: text
    description: "Work title"
  - name: publication_year
    type: integer
    description: "Year of publication"
  - name: publication_date
    type: date
    description: "Full publication date"
  - name: cited_by_count
    type: integer
    description: "Total citation count from OpenAlex"
    checks:
      - name: positive
  - name: concepts
    type: jsonb
    description: "JSON array of OpenAlex concept objects with scores"
  - name: authorships
    type: jsonb
    description: "JSON array of authorship objects with institution data"
  - name: referenced_works
    type: jsonb
    description: "JSON array of OpenAlex work IDs cited by this work"
  - name: source
    type: text
    description: "Data source identifier"
  - name: raw_payload
    type: jsonb
    description: "Full raw JSON payload from Kafka"
  - name: ingested_at
    type: timestamptz
    description: "Timestamp when record was written to landing"
  - name: kafka_offset
    type: bigint
    description: "Kafka message offset"
  - name: kafka_partition
    type: integer
    description: "Kafka partition"

depends:
  []
@bruin */

SELECT
    work_id,
    doi,
    title,
    publication_year,
    publication_date,
    cited_by_count,
    concepts,
    authorships,
    referenced_works,
    source,
    raw_payload,
    ingested_at,
    kafka_offset,
    kafka_partition
FROM raw_openalex_works
WHERE work_id IS NOT NULL
