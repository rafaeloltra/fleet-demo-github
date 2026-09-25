# Consumed by seed-data/generate_seed_data.py's --produce mode, which needs
# to wire-encode records (Confluent's magic-byte + schema-id header) since
# the raw/context topics use 'value.format' = 'json-registry'. Pull these
# with `terraform output -raw <name>` (add `-json` for the whole set).

output "kafka_rest_endpoint" {
  value = confluent_kafka_cluster.fleet.rest_endpoint
}

# Native Kafka protocol bootstrap server (port 9092) - distinct from
# kafka_rest_endpoint above (port 443, Kafka REST Proxy v3, HTTP). Consumed
# by fleet-context-engine's kafkajs-based background consumer, which needs the
# real Kafka wire protocol to continuously tail topics rather than
# re-querying Flink per request.
output "kafka_bootstrap_endpoint" {
  value = confluent_kafka_cluster.fleet.bootstrap_endpoint
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.fleet.id
}

output "schema_registry_rest_endpoint" {
  value = data.confluent_schema_registry_cluster.fleet.rest_endpoint
}

output "app_manager_kafka_api_key" {
  value     = confluent_api_key.app_manager_kafka_key.id
  sensitive = true
}

output "app_manager_kafka_api_secret" {
  value     = confluent_api_key.app_manager_kafka_key.secret
  sensitive = true
}

# Read-only identity for fleet-context-engine's background consumer - already
# has DeveloperRead on all topics + groups (topics.tf), matching its
# original stated purpose ("Read-only identity for Sumo Logic dashboards /
# downstream consumers") exactly.
output "sumologic_reader_kafka_api_key" {
  value     = confluent_api_key.sumologic_reader_key.id
  sensitive = true
}

output "sumologic_reader_kafka_api_secret" {
  value     = confluent_api_key.sumologic_reader_key.secret
  sensitive = true
}

output "app_manager_schema_registry_api_key" {
  value     = confluent_api_key.app_manager_sr_key.id
  sensitive = true
}

output "app_manager_schema_registry_api_secret" {
  value     = confluent_api_key.app_manager_sr_key.secret
  sensitive = true
}

# Consumed by fleet-context-engine (context-engine.js) to query the real
# Flink pipeline (SQL pull queries) and to produce real demo-injection
# records (the HTTP API's /api/inject-incident and
# /api/inject-traffic-incident endpoints), instead of the in-memory
# simulator.

output "flink_rest_endpoint" {
  value = data.confluent_flink_region.fleet.rest_endpoint
}

output "flink_organization_id" {
  value = data.confluent_organization.fleet.id
}

output "flink_environment_id" {
  value = confluent_environment.fleet.id
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.ai_advisor.id
}

output "flink_principal_id" {
  value = confluent_service_account.flink_runner.id
}

output "flink_catalog_name" {
  value = confluent_environment.fleet.display_name
}

output "flink_database_name" {
  value = confluent_kafka_cluster.fleet.display_name
}

output "flink_api_key" {
  value     = confluent_api_key.flink_runner_key.id
  sensitive = true
}

output "flink_api_secret" {
  value     = confluent_api_key.flink_runner_key.secret
  sensitive = true
}

# Consumed when wiring up Snowflake's ICEBERG_REST catalog integration
# (tableflow.tf) - see setup-demo.md/README for the exact Snowflake-side
# CREATE CATALOG INTEGRATION statement these feed into.

output "tableflow_rest_catalog_endpoint" {
  value = "https://tableflow.${var.cluster_region}.${lower(var.cluster_cloud)}.confluent.cloud/iceberg/catalog/organizations/${data.confluent_organization.fleet.id}/environments/${confluent_environment.fleet.id}"
}

output "tableflow_catalog_namespace" {
  value = confluent_kafka_cluster.fleet.id
}

output "tableflow_api_key" {
  value     = confluent_api_key.tableflow_key.id
  sensitive = true
}

output "tableflow_api_secret" {
  value     = confluent_api_key.tableflow_key.secret
  sensitive = true
}
