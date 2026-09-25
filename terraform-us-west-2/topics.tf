# The 10 topics for the fan-out architecture (one raw vehicle ingest topic,
# three external context topics, three business-domain topics, three
# AI-recommendation topics) are created by Flink itself via `CREATE TABLE`
# in flink_statements.tf, not here - see that file's header comment for why.
# This file only holds the Kafka-cluster RBAC for the service accounts that
# produce/consume that data directly (not through Flink).

resource "confluent_role_binding" "app_manager_cluster_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.fleet.rbac_crn
}

locals {
  raw_topic_name = "vehicle.telemetry"
}

# Producer: only needs to write the raw telemetry topic.
resource "confluent_role_binding" "vehicle_simulator_write" {
  principal   = "User:${confluent_service_account.vehicle_simulator.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.fleet.rbac_crn}/kafka=${confluent_kafka_cluster.fleet.id}/topic=${local.raw_topic_name}"
}

# Sumo Logic / downstream consumer: read-only across all topics it
# dashboards on, plus read on its consumer group.
resource "confluent_role_binding" "sumologic_reader_read_topics" {
  principal   = "User:${confluent_service_account.sumologic_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.fleet.rbac_crn}/kafka=${confluent_kafka_cluster.fleet.id}/topic=*"
}

resource "confluent_role_binding" "sumologic_reader_read_groups" {
  principal   = "User:${confluent_service_account.sumologic_reader.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.fleet.rbac_crn}/kafka=${confluent_kafka_cluster.fleet.id}/group=*"
}
