# Tableflow materializes the 6 downstream topics (3 business-domain, 3 AI
# recommendation) as Iceberg tables in Confluent-managed storage, so they can
# be queried directly from Snowflake as a native ICEBERG_REST catalog
# integration - no external catalog service (Snowflake Open Catalog/Polaris,
# AWS Glue) required. The raw/context topics (vehicle.telemetry,
# weather.conditions, traffic.incidents, parcel.volume) are intentionally
# left out - they're high-volume/low business-value for analytics.
#
# app_manager already holds CloudClusterAdmin on the cluster (topics.tf),
# which is sufficient to enable/manage Tableflow topics - no new role
# binding needed.

resource "confluent_api_key" "tableflow_key" {
  display_name = "app-manager-tableflow-key"
  description  = "Tableflow API key owned by app-manager, used to enable/manage Tableflow topics and to authenticate Snowflake's Iceberg REST catalog integration."
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
  }

  lifecycle {
    prevent_destroy = true
  }
}

locals {
  tableflow_topics = [
    "maintenance.alerts",
    "driver.risk.events",
    "delivery.performance.events",
    "ai.maintenance.recommendations",
    "ai.safety.recommendations",
    "ai.delivery.recommendations",
  ]
}

resource "confluent_tableflow_topic" "fleet" {
  for_each = toset(local.tableflow_topics)

  environment {
    id = confluent_environment.fleet.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.fleet.id
  }

  display_name  = each.value
  table_formats = ["ICEBERG"]

  managed_storage {}

  credentials {
    key    = confluent_api_key.tableflow_key.id
    secret = confluent_api_key.tableflow_key.secret
  }

  lifecycle {
    prevent_destroy = true
  }
}
