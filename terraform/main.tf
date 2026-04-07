# Research Intelligence Pipeline — Terraform
# Provisions Supabase PostgreSQL tables via the PostgreSQL provider.
# Supabase project itself must be created manually via the Supabase dashboard.

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.22"
    }
  }
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "supabase_db_host" {
  description = "Supabase PostgreSQL host (e.g. db.xxxx.supabase.co)"
  type        = string
}

variable "supabase_db_password" {
  description = "Supabase PostgreSQL password"
  type        = string
  sensitive   = true
}

variable "supabase_db_name" {
  description = "Database name"
  type        = string
  default     = "postgres"
}

variable "supabase_db_user" {
  description = "Database user"
  type        = string
  default     = "postgres"
}

# ── Provider ──────────────────────────────────────────────────────────────────

provider "postgresql" {
  host            = var.supabase_db_host
  port            = 5432
  database        = var.supabase_db_name
  username        = var.supabase_db_user
  password        = var.supabase_db_password
  sslmode         = "require"
  connect_timeout = 15
}

# ── Schemas ───────────────────────────────────────────────────────────────────

resource "postgresql_schema" "landing" {
  name = "landing"
}

resource "postgresql_schema" "staging" {
  name       = "staging"
  depends_on = [postgresql_schema.landing]
}

resource "postgresql_schema" "intermediate" {
  name       = "intermediate"
  depends_on = [postgresql_schema.staging]
}

resource "postgresql_schema" "marts" {
  name       = "marts"
  depends_on = [postgresql_schema.intermediate]
}

# ── Landing Tables ────────────────────────────────────────────────────────────

resource "postgresql_extension" "pg_trgm" {
  name = "pg_trgm"
}

resource "postgresql_extension" "unaccent" {
  name = "unaccent"
}

# raw_arxiv_papers is created by the consumer on startup via DDL.
# We declare it here for full IaC tracking.
resource "postgresql_grant" "landing_usage" {
  database    = var.supabase_db_name
  role        = var.supabase_db_user
  schema      = postgresql_schema.landing.name
  object_type = "schema"
  privileges  = ["USAGE", "CREATE"]
}

resource "postgresql_grant" "staging_usage" {
  database    = var.supabase_db_name
  role        = var.supabase_db_user
  schema      = postgresql_schema.staging.name
  object_type = "schema"
  privileges  = ["USAGE", "CREATE"]
}

resource "postgresql_grant" "intermediate_usage" {
  database    = var.supabase_db_name
  role        = var.supabase_db_user
  schema      = postgresql_schema.intermediate.name
  object_type = "schema"
  privileges  = ["USAGE", "CREATE"]
}

resource "postgresql_grant" "marts_usage" {
  database    = var.supabase_db_name
  role        = var.supabase_db_user
  schema      = postgresql_schema.marts.name
  object_type = "schema"
  privileges  = ["USAGE", "CREATE"]
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "schemas_created" {
  description = "List of schemas provisioned"
  value = [
    postgresql_schema.landing.name,
    postgresql_schema.staging.name,
    postgresql_schema.intermediate.name,
    postgresql_schema.marts.name,
  ]
}
