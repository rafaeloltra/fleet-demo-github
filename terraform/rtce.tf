# Turns on Confluent Cloud's Real-Time Context Engine (RTCE) for every
# topic in the fan-out pipeline, so MCP clients (see
# fleet-context-engine/SETUP-RTCE-COPILOT.md) can query them directly without
# the `confluent rtce rtce-topic create` CLI step. Requires provider
# >= 2.73.0 (see versions.tf) and RTCE availability in cluster_region.
locals {
  rtce_topics = {
    "vehicle.telemetry"              = "Raw per-vehicle GPS/engine/sensor telemetry, one record per report."
    "weather.conditions"             = "External weather context per geozone."
    "traffic.incidents"              = "External traffic incident context per geozone."
    "parcel.volume"                  = "External parcel-volume context per geozone."
    "maintenance.alerts"             = "Derived per-vehicle maintenance risk signals."
    "driver.risk.events"             = "Derived per-vehicle/driver safety risk events."
    "delivery.performance.events"    = "Derived per-vehicle delivery performance events."
    "ai.maintenance.recommendations" = "AI-generated maintenance recommendations."
    "ai.safety.recommendations"      = "AI-generated driver safety recommendations."
    "ai.delivery.recommendations"    = "AI-generated delivery recommendations."
  }
}

resource "confluent_rtce_topic" "fleet" {
  for_each = local.rtce_topics

  cloud       = var.cluster_cloud
  region      = var.cluster_region
  topic_name  = each.key
  description = each.value

  environment {
    id = confluent_environment.fleet.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.fleet.id
  }

  # topic_name/description above are plain strings, not attributes of the
  # confluent_flink_statement resources below - Terraform's dependency graph
  # has no implicit edge to them, so without this explicit depends_on, RTCE
  # enablement could run concurrently with (or before) Flink actually
  # creates each topic + registers its schema via CREATE TABLE, which is
  # exactly the scenario RTCE needs to already be done.
  depends_on = [
    confluent_flink_statement.create_vehicle_telemetry,
    confluent_flink_statement.create_weather_conditions,
    confluent_flink_statement.create_traffic_incidents,
    confluent_flink_statement.create_parcel_volume,
    confluent_flink_statement.create_maintenance_alerts,
    confluent_flink_statement.create_driver_risk_events,
    confluent_flink_statement.create_delivery_performance_events,
    confluent_flink_statement.create_ai_maintenance_recommendations,
    confluent_flink_statement.create_ai_safety_recommendations,
    confluent_flink_statement.create_ai_delivery_recommendations,
  ]
}
