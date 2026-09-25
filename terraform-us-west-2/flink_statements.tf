# Flink SQL pipeline: creates all 10 topics as structured tables (with a
# JSON schema registered in Schema Registry), derives the 3 business-domain
# topics from the raw vehicle.telemetry / parcel.volume streams, then
# derives the 3 AI-recommendation topics via the OpenAI connection
# (flink.tf). All statements run continuously as the AI Advisor service
# account (sa-flink-ai-advisor).
#
# Topics are created here with `CREATE TABLE ... WITH ('value.format' =
# 'json-registry')` rather than pre-created via confluent_kafka_topic +
# `ALTER TABLE ... ADD`. That retrofit path was tried first and doesn't
# work: a topic with no schema is permanently treated as Flink's 'raw'
# format (a single opaque BYTES column), and 'raw' format only accepts one
# flat scalar column - full nested access (ROW/ARRAY, matching real JSON)
# is impossible on it. Registering a Schema Registry schema for the subject
# after the fact - or even before the topic is ever read, or after
# producing a real schema-registry-wire-format message to it - does not
# change this once Flink has already cached the topic as 'raw'. `CREATE
# TABLE` sidesteps all of that by having Flink create the topic and its
# schema together from the start.

locals {
  flink_statement_defaults = {
    "sql.current-catalog"  = confluent_environment.fleet.display_name
    "sql.current-database" = confluent_kafka_cluster.fleet.display_name
  }

  # Confluent Cloud Flink needs a fully-qualified <catalog>.<database>.<table>
  # reference to resolve topic names that contain dots (e.g. `vehicle.telemetry`)
  # as a single table identifier rather than a catalog.database path.
  db = "`${confluent_environment.fleet.display_name}`.`${confluent_kafka_cluster.fleet.display_name}`"
}

# --- Create all 10 topics as structured tables -----------------------------
#
# vehicle.telemetry's shape is taken directly from the project's real sample
# Kafka export ("Download messages as CSV...") rather than the reduced shape
# in seed-data/generate_seed_data.py, so it matches actual production data.

resource "confluent_flink_statement" "create_vehicle_telemetry" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`vehicle.telemetry` (
      object_id STRING,
      `datetime` STRING,
      ignition_status STRING,
      trip_type STRING,
      `position` ROW<
        altitude DOUBLE,
        longitude DOUBLE,
        latitude DOUBLE,
        direction DOUBLE,
        satellites_count INT,
        speed DOUBLE
      >,
      inputs ROW<
        other ROW<
          country_code_geonames DOUBLE,
          virtual_gps_odometer DOUBLE
        >,
        calculated_inputs ROW<
          fuel_consumption DOUBLE,
          fuel_level DOUBLE,
          mileage DOUBLE,
          rpm DOUBLE,
          temperature DOUBLE,
          weight DOUBLE
        >,
        device_inputs ROW<
          priority STRING,
          movement STRING,
          hdop STRING,
          power_supply_voltage DOUBLE,
          battery_voltage DOUBLE,
          gps_speed DOUBLE,
          gsm_signal_strength DOUBLE,
          `operator` DOUBLE,
          engine_rpm DOUBLE,
          canbus_engine_coolant_temperature DOUBLE,
          fuel_level_can DOUBLE,
          speed_wheel DOUBLE,
          overspeeding_events STRING,
          ecodrive_braking_events DOUBLE,
          ecodrive_harsh_acceleration DOUBLE,
          ecodrive_idling_time DOUBLE,
          lcv_driver_doors STRING,
          lcv_left_back_doors STRING,
          lcv_right_back_doors STRING
        >,
        tires ARRAY<ROW<
          tire_id STRING,
          tire_pressure DOUBLE,
          tire_temperature DOUBLE,
          tire_air_leakage_rate DOUBLE,
          tire_pressure_threshold_detection INT,
          tire_status INT,
          tire_sensor_electrical_fault INT,
          tire_sensor_enable_status INT,
          tire_location INT,
          tire_extended_tire_pressure_support INT
        >>
      >,
      geozone_ids ARRAY<STRING>
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_weather_conditions" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`weather.conditions` (
      geozone STRING,
      `condition` STRING,
      temp_c DOUBLE,
      wind_kph DOUBLE,
      updated_at STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_traffic_incidents" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`traffic.incidents` (
      geozone STRING,
      `type` STRING,
      severity STRING,
      `timestamp` STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_parcel_volume" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`parcel.volume` (
      vehicle_id STRING,
      geozone STRING,
      parcels_onboard INT,
      parcels_delivered_last_hour INT,
      `timestamp` STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_maintenance_alerts" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`maintenance.alerts` (
      vehicle_id STRING,
      geozone STRING,
      engine_temp_c DOUBLE,
      severity STRING,
      message STRING,
      event_time STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_driver_risk_events" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`driver.risk.events` (
      vehicle_id STRING,
      geozone STRING,
      speed_kmh DOUBLE,
      overspeed_status STRING,
      harsh_braking_score DOUBLE,
      risk_level STRING,
      event_time STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_delivery_performance_events" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`delivery.performance.events` (
      vehicle_id STRING,
      geozone STRING,
      parcels_onboard INT,
      parcels_delivered_last_hour INT,
      performance_status STRING,
      event_time STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_ai_maintenance_recommendations" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`ai.maintenance.recommendations` (
      vehicle_id STRING,
      geozone STRING,
      severity STRING,
      trigger_event STRING,
      recommendation STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_ai_safety_recommendations" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`ai.safety.recommendations` (
      vehicle_id STRING,
      geozone STRING,
      risk_level STRING,
      speed_kmh DOUBLE,
      recommendation STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "create_ai_delivery_recommendations" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`ai.delivery.recommendations` (
      vehicle_id STRING,
      geozone STRING,
      performance_status STRING,
      parcels_onboard INT,
      recommendation STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

# --- Business-domain topics, derived from raw telemetry / parcel data -----

resource "confluent_flink_statement" "derive_maintenance_alerts" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`maintenance.alerts`
    SELECT
      object_id AS vehicle_id,
      geozone_ids[1] AS geozone,
      inputs.calculated_inputs.temperature AS engine_temp_c,
      CASE WHEN inputs.calculated_inputs.temperature > 105 THEN 'high' ELSE 'medium' END AS severity,
      CONCAT('Vehicle ', object_id, ' engine temperature ', CAST(inputs.calculated_inputs.temperature AS STRING), 'C near ', geozone_ids[1]) AS message,
      `datetime` AS event_time
    FROM ${local.db}.`vehicle.telemetry`
    WHERE inputs.calculated_inputs.temperature > 100
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.create_vehicle_telemetry,
    confluent_flink_statement.create_maintenance_alerts,
  ]
}

resource "confluent_flink_statement" "derive_driver_risk_events" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`driver.risk.events`
    SELECT
      object_id AS vehicle_id,
      geozone_ids[1] AS geozone,
      `position`.speed AS speed_kmh,
      inputs.device_inputs.overspeeding_events AS overspeed_status,
      inputs.device_inputs.ecodrive_braking_events AS harsh_braking_score,
      CASE
        WHEN inputs.device_inputs.overspeeding_events = 'OVERSPEED' AND inputs.device_inputs.ecodrive_braking_events > 0.7 THEN 'high'
        WHEN inputs.device_inputs.overspeeding_events = 'OVERSPEED' OR inputs.device_inputs.ecodrive_braking_events > 0.7 THEN 'medium'
        ELSE 'low'
      END AS risk_level,
      `datetime` AS event_time
    FROM ${local.db}.`vehicle.telemetry`
    WHERE inputs.device_inputs.overspeeding_events = 'OVERSPEED' OR inputs.device_inputs.ecodrive_braking_events > 0.7
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.create_vehicle_telemetry,
    confluent_flink_statement.create_driver_risk_events,
  ]
}

resource "confluent_flink_statement" "derive_delivery_performance_events" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`delivery.performance.events`
    SELECT
      vehicle_id,
      geozone,
      parcels_onboard,
      parcels_delivered_last_hour,
      CASE
        WHEN parcels_onboard > 70 AND parcels_delivered_last_hour < 5 THEN 'behind_schedule'
        WHEN parcels_delivered_last_hour >= 15 THEN 'ahead_of_schedule'
        ELSE 'on_schedule'
      END AS performance_status,
      `timestamp` AS event_time
    FROM ${local.db}.`parcel.volume`
    WHERE parcels_onboard > 70 OR parcels_delivered_last_hour >= 15
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.create_parcel_volume,
    confluent_flink_statement.create_delivery_performance_events,
  ]
}

# --- AI recommendation model + topics, derived from the business events ---

resource "confluent_flink_statement" "openai_advisor_model" {
  statement = <<-SQL
    CREATE MODEL ${local.db}.fleet_ai_advisor
    INPUT (prompt STRING)
    OUTPUT (recommendation STRING)
    WITH (
      'provider' = 'openai',
      'task' = 'text_generation',
      'openai.connection' = 'fleet-openai-connection',
      'openai.model_version' = '${var.openai_model_id}'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [confluent_flink_connection.openai]
}

resource "confluent_flink_statement" "derive_ai_maintenance_recommendations" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`ai.maintenance.recommendations`
    SELECT
      m.vehicle_id,
      m.geozone,
      m.severity,
      m.message AS trigger_event,
      p.recommendation
    FROM ${local.db}.`maintenance.alerts` AS m,
    LATERAL TABLE(ML_PREDICT('fleet_ai_advisor', CONCAT('Fleet maintenance alert - ', m.message, '. In one short sentence, recommend the next maintenance action.'))) AS p(recommendation)
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.openai_advisor_model,
    confluent_flink_statement.derive_maintenance_alerts,
    confluent_flink_statement.create_ai_maintenance_recommendations,
  ]
}

resource "confluent_flink_statement" "derive_ai_safety_recommendations" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`ai.safety.recommendations`
    SELECT
      d.vehicle_id,
      d.geozone,
      d.risk_level,
      d.speed_kmh,
      p.recommendation
    FROM ${local.db}.`driver.risk.events` AS d,
    LATERAL TABLE(ML_PREDICT('fleet_ai_advisor', CONCAT('Driver safety alert for vehicle ', d.vehicle_id, ' in ', d.geozone, ' - speed ', CAST(d.speed_kmh AS STRING), 'km/h, overspeed status ', d.overspeed_status, ', harsh braking score ', CAST(d.harsh_braking_score AS STRING), '. In one short sentence, recommend a safety action.'))) AS p(recommendation)
    WHERE d.risk_level IN ('high', 'medium')
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.openai_advisor_model,
    confluent_flink_statement.derive_driver_risk_events,
    confluent_flink_statement.create_ai_safety_recommendations,
  ]
}

resource "confluent_flink_statement" "derive_ai_delivery_recommendations" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`ai.delivery.recommendations`
    SELECT
      e.vehicle_id,
      e.geozone,
      e.performance_status,
      e.parcels_onboard,
      p.recommendation
    FROM ${local.db}.`delivery.performance.events` AS e,
    LATERAL TABLE(ML_PREDICT('fleet_ai_advisor', CONCAT('Delivery performance alert for vehicle ', e.vehicle_id, ' in ', e.geozone, ' - status ', e.performance_status, ', ', CAST(e.parcels_onboard AS STRING), ' parcels onboard. In one short sentence, recommend a delivery-ops action.'))) AS p(recommendation)
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.openai_advisor_model,
    confluent_flink_statement.derive_delivery_performance_events,
    confluent_flink_statement.create_ai_delivery_recommendations,
  ]
}

# --- AI_DETECT_ANOMALIES experiment: replaces the fixed-threshold heuristic
# in derive_driver_risk_events above with per-vehicle statistical anomaly
# detection, so each vehicle is judged against its own driving baseline
# instead of one global overspeed/harsh-braking cutoff. Written to a
# separate driver.risk.events.anomaly topic (not a swap-in-place) so the
# heuristic feed keeps running unchanged and the two can be compared
# side by side in the console/demo.
#
# AI_DETECT_ANOMALIES(DOUBLE, TIMESTAMP(3)) needs its second argument to be
# a genuine watermarked event-time attribute for its internal OVER
# aggregation to accumulate state correctly across rows - a computed
# TO_TIMESTAMP(...) expression ordered without its own declared watermark
# was tested and silently produces null/single-row output (empirically
# confirmed against the AI_DETECT_ANOMALIES rollout in rtce-bug-bash before
# writing this). vehicle.telemetry's `datetime` is a plain STRING, so this
# intermediate table parses it once into a real TIMESTAMP(3) column with
# its own watermark for AI_DETECT_ANOMALIES to key off.
#
# The CREATE TABLE below still declares a zero-lag watermark
# (`WATERMARK FOR event_ts AS event_ts`), but the live table's watermark was
# adjusted out-of-band via `ALTER TABLE vehicle_telemetry_ts MODIFY WATERMARK
# FOR event_ts AS event_ts - INTERVAL '0.001' SECOND` - the telemetry
# simulator produces all 12 vehicles on the same 3s tick and truncates
# `datetime` to whole seconds, so several vehicles legitimately land on the
# identical per-second timestamp; a zero-lag watermark treats ties at the
# current watermark as late data (Confluent's console surfaces this as a
# "degraded" statement warning). Do not "fix" this by re-editing the
# `statement` text below - the confluent_flink_statement resource treats
# `statement` as force-new, so any edit here drops and recreates this topic
# (and disconnects the two statements below that already point at it) rather
# than updating the live watermark in place.

resource "confluent_flink_statement" "create_vehicle_telemetry_ts" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.vehicle_telemetry_ts (
      vehicle_id STRING,
      geozone STRING,
      event_ts TIMESTAMP(3),
      speed_kmh DOUBLE,
      WATERMARK FOR event_ts AS event_ts
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "derive_vehicle_telemetry_ts" {
  statement = <<-SQL
    INSERT INTO ${local.db}.vehicle_telemetry_ts
    SELECT
      object_id AS vehicle_id,
      geozone_ids[1] AS geozone,
      TO_TIMESTAMP(SUBSTRING(REPLACE(`datetime`, 'T', ' '), 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS') AS event_ts,
      `position`.speed AS speed_kmh
    FROM ${local.db}.`vehicle.telemetry`
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.create_vehicle_telemetry,
    confluent_flink_statement.create_vehicle_telemetry_ts,
  ]
}

resource "confluent_flink_statement" "create_driver_risk_events_anomaly" {
  statement = <<-SQL
    CREATE TABLE ${local.db}.`driver.risk.events.anomaly` (
      vehicle_id STRING,
      geozone STRING,
      speed_kmh DOUBLE,
      forecast_speed_kmh DOUBLE,
      lower_bound DOUBLE,
      upper_bound DOUBLE,
      risk_level STRING,
      event_time STRING
    ) WITH (
      'value.format' = 'json-registry'
    )
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_cluster_admin,
    confluent_role_binding.flink_runner_schema_registry,
  ]
}

resource "confluent_flink_statement" "derive_driver_risk_events_anomaly" {
  statement = <<-SQL
    INSERT INTO ${local.db}.`driver.risk.events.anomaly`
    SELECT
      vehicle_id,
      geozone,
      speed_kmh,
      anomaly.forecast_value AS forecast_speed_kmh,
      anomaly.lower_bound AS lower_bound,
      anomaly.upper_bound AS upper_bound,
      CASE
        WHEN ABS(speed_kmh - anomaly.forecast_value) > (anomaly.upper_bound - anomaly.lower_bound) THEN 'high'
        ELSE 'medium'
      END AS risk_level,
      CAST(event_ts AS STRING) AS event_time
    FROM (
      SELECT
        vehicle_id,
        geozone,
        event_ts,
        speed_kmh,
        AI_DETECT_ANOMALIES(speed_kmh, event_ts) OVER (PARTITION BY vehicle_id ORDER BY event_ts) AS anomaly
      FROM ${local.db}.vehicle_telemetry_ts
    )
    WHERE anomaly.is_anomaly = TRUE
  SQL

  properties = local.flink_statement_defaults

  compute_pool { id = confluent_flink_compute_pool.ai_advisor.id }
  principal { id = confluent_service_account.flink_runner.id }
  environment { id = confluent_environment.fleet.id }
  organization { id = data.confluent_organization.fleet.id }
  credentials {
    key    = confluent_api_key.flink_runner_key.id
    secret = confluent_api_key.flink_runner_key.secret
  }
  rest_endpoint = data.confluent_flink_region.fleet.rest_endpoint

  depends_on = [
    confluent_flink_statement.derive_vehicle_telemetry_ts,
    confluent_flink_statement.create_driver_risk_events_anomaly,
  ]
}
