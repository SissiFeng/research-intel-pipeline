/* asset: landing.raw_arxiv_papers
 *
 * Landing table for raw arXiv paper records ingested via Kafka consumer.
 * This asset registers the table with Bruin for lineage tracking.
 * The actual table DDL is managed by the Kafka consumer on startup.
 *
 * Upstream: Redpanda topic raw_arxiv_papers (via arxiv_consumer.py)
 * Downstream: staging.stg_papers
 */

/* @bruin
name: landing.raw_arxiv_papers
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
    description: "arXiv paper identifier (e.g. 2301.00001v1)"
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: text
    description: "Paper title"
    checks:
      - name: not_null
  - name: abstract
    type: text
    description: "Paper abstract text"
  - name: authors
    type: jsonb
    description: "JSON array of author name strings"
  - name: categories
    type: jsonb
    description: "JSON array of arXiv category codes"
  - name: published
    type: timestamptz
    description: "Original publication timestamp"
  - name: updated
    type: timestamptz
    description: "Last updated timestamp on arXiv"
  - name: doi
    type: text
    description: "DOI if available"
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
    description: "Kafka message offset for audit trail"
  - name: kafka_partition
    type: integer
    description: "Kafka partition for audit trail"

depends:
  []
@bruin */

SELECT
    paper_id,
    title,
    abstract,
    authors,
    categories,
    published,
    updated,
    doi,
    source,
    raw_payload,
    ingested_at,
    kafka_offset,
    kafka_partition
FROM raw_arxiv_papers
WHERE paper_id IS NOT NULL
