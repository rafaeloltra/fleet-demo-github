# Flink region - used to scope the Flink API key and to look up the Flink REST endpoint
# for this cloud/region (Flink connections/statements authenticate against this endpoint,
# separately from the Cloud API key used for the rest of this config).
data "confluent_flink_region" "fleet" {
  cloud  = var.flink_cloud
  region = var.flink_region
}

data "confluent_organization" "fleet" {}

# Auto-provisioned by the environment's stream_governance block (environment.tf).
data "confluent_schema_registry_cluster" "fleet" {
  environment {
    id = confluent_environment.fleet.id
  }
}

# Flink compute pool - must live in the same cloud/region as the Kafka cluster.
resource "confluent_flink_compute_pool" "ai_advisor" {
  display_name = "fleet-ai-advisor-pool"
  cloud        = var.flink_cloud
  region       = var.flink_region
  max_cfu      = var.flink_cfu

  environment {
    id = confluent_environment.fleet.id
  }
}

# API key Flink statements use to run against the compute pool.
resource "confluent_api_key" "flink_runner_key" {
  display_name = "flink-ai-advisor-key"
  description  = "Flink API key owned by the AI Advisor service account."
  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.fleet.id
    api_version = data.confluent_flink_region.fleet.api_version
    kind        = data.confluent_flink_region.fleet.kind
    environment {
      id = confluent_environment.fleet.id
    }
  }
}

# Grants the AI Advisor service account permission to manage Flink resources
# (compute pools, connections, statements) in this environment - without this,
# creating confluent_flink_connection.bedrock fails with a 403.
resource "confluent_role_binding" "flink_runner_admin" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "FlinkAdmin"
  crn_pattern = confluent_environment.fleet.resource_name
}

# Required for Flink to resolve the Kafka cluster as a queryable database at
# all (independent of the topic-level ACLs/role bindings in topics.tf) -
# without this, every Flink statement fails with "A database with name or
# id '...' cannot be resolved", even for SHOW TABLES.
resource "confluent_role_binding" "flink_runner_cluster_admin" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.fleet.rbac_crn
}

# Required for `CREATE TABLE ... WITH ('value.format' = 'json-registry')` in
# flink_statements.tf - Flink registers/reads each topic's value schema in
# Schema Registry as the flink_runner principal, which needs subject-level
# access to do so.
resource "confluent_role_binding" "flink_runner_schema_registry" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "crn://confluent.cloud/organization=${data.confluent_organization.fleet.id}/environment=${confluent_environment.fleet.id}/schema-registry=${data.confluent_schema_registry_cluster.fleet.id}/subject=*"
}

# ---------------------------------------------------------------------------
# Bedrock connection: this replaces the Vertex AI connection from the GCP
# version of this demo. Flink SQL statements call this connection by name,
# e.g.:
#
#   SELECT object_id,
#          AI_MODEL('fleet-bedrock-connection', context_json) AS recommendation
#   FROM ai_advisor_context
#   WHERE maintenance_risk > 70;
#
# Note: as of the current Confluent Cloud Flink AI Model docs, Bedrock
# connections authenticate with a standard AWS access key/secret pair
# (an IAM user scoped to bedrock:InvokeModel only - do not use root/admin
# credentials). If your org instead wants to use an IAM role/instance
# profile, check the latest confluent_flink_connection docs, since
# Confluent has been actively expanding auth options for this resource.
# ---------------------------------------------------------------------------
resource "confluent_flink_connection" "bedrock" {
  display_name = "fleet-bedrock-connection"
  type         = "BEDROCK"

  endpoint     = "https://bedrock-runtime.${var.bedrock_region}.amazonaws.com/model/${var.bedrock_model_id}/invoke"
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  aws_access_key = var.bedrock_aws_access_key_id
  aws_secret_key = var.bedrock_aws_secret_access_key

  environment {
    id = confluent_environment.fleet.id
  }

  organization {
    id = data.confluent_organization.fleet.id
  }

  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }

  compute_pool {
    id = confluent_flink_compute_pool.ai_advisor.id
  }

  principal {
    id = confluent_service_account.flink_runner.id
  }

  depends_on = [confluent_role_binding.flink_runner_admin]
}
