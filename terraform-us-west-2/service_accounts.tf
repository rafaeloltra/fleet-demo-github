# One service account per "actor" in the architecture diagram. This makes
# the demo easy to reason about and mirrors real production practice
# (least-privilege, one identity per app).

resource "confluent_service_account" "app_manager" {
  display_name = "sa-fleet-app-manager-uswest2"
  description  = "Manages topics, ACLs and cluster config via Terraform."
}

resource "confluent_service_account" "vehicle_simulator" {
  display_name = "sa-vehicle-simulator-uswest2"
  description  = "Publishes raw FMS telemetry events to vehicle.telemetry."
}

resource "confluent_service_account" "flink_runner" {
  display_name = "sa-flink-ai-advisor-uswest2"
  description  = "Used by Flink statements to read source topics, call OpenAI, and write ai.* topics."
}

resource "confluent_service_account" "sumologic_reader" {
  display_name = "sa-sumologic-consumer-uswest2"
  description  = "Read-only identity for Sumo Logic dashboards / downstream consumers."
}

# Cluster-admin API key for Terraform itself to manage topics/ACLs.
resource "confluent_api_key" "app_manager_kafka_key" {
  display_name = "app-manager-kafka-key"
  description  = "Kafka API key owned by app-manager service account."
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.fleet.id
    api_version = confluent_kafka_cluster.fleet.api_version
    kind        = confluent_kafka_cluster.fleet.kind
    environment {
      id = confluent_environment.fleet.id
    }
  }
}

# Producer key: give this to your Python vehicle simulator script.
resource "confluent_api_key" "vehicle_simulator_key" {
  display_name = "vehicle-simulator-key"
  description  = "Kafka API key for the vehicle simulator producer."
  owner {
    id          = confluent_service_account.vehicle_simulator.id
    api_version = confluent_service_account.vehicle_simulator.api_version
    kind        = confluent_service_account.vehicle_simulator.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.fleet.id
    api_version = confluent_kafka_cluster.fleet.api_version
    kind        = confluent_kafka_cluster.fleet.kind
    environment {
      id = confluent_environment.fleet.id
    }
  }
}

# Schema Registry access for app_manager: needed so seed-data/generate_seed_data.py
# can look up each topic's registered schema ID and wire-encode records
# correctly (the 4 raw/context topics use 'value.format' = 'json-registry',
# so plain JSON bytes without the Confluent wire-format header/schema ID are
# not readable by Flink - see flink_statements.tf).
resource "confluent_role_binding" "app_manager_schema_registry" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "crn://confluent.cloud/organization=${data.confluent_organization.fleet.id}/environment=${confluent_environment.fleet.id}/schema-registry=${data.confluent_schema_registry_cluster.fleet.id}/subject=*"
}

resource "confluent_api_key" "app_manager_sr_key" {
  display_name = "app-manager-schema-registry-key"
  description  = "Schema Registry API key owned by app-manager, used by seed-data/generate_seed_data.py to look up schema IDs."
  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.fleet.id
    api_version = data.confluent_schema_registry_cluster.fleet.api_version
    kind        = data.confluent_schema_registry_cluster.fleet.kind
    environment {
      id = confluent_environment.fleet.id
    }
  }
}

# Consumer key: give this to Sumo Logic's Kafka source connector / collector.
resource "confluent_api_key" "sumologic_reader_key" {
  display_name = "sumologic-reader-key"
  description  = "Kafka API key for Sumo Logic dashboards / downstream consumers."
  owner {
    id          = confluent_service_account.sumologic_reader.id
    api_version = confluent_service_account.sumologic_reader.api_version
    kind        = confluent_service_account.sumologic_reader.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.fleet.id
    api_version = confluent_kafka_cluster.fleet.api_version
    kind        = confluent_kafka_cluster.fleet.kind
    environment {
      id = confluent_environment.fleet.id
    }
  }
}
