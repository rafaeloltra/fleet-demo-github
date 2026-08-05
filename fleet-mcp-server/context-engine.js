// context-engine.js
//
// Reads the REAL fleet state from Confluent Cloud + Flink + Bedrock (the
// pipeline in terraform/), instead of simulating it in memory.
//
// A background Kafka consumer (kafkajs) tails all 10 real topics from the
// beginning and continuously rebuilds the same in-memory shape the
// original simulator used (vehicles Map, weather Map, rolling logs, etc.),
// so every public method here is a synchronous, instant read from memory -
// same as before. Running Flink SQL pull queries per tool call was tried
// first and is too slow/contends with the pipeline's own compute pool
// capacity for an interactive demo (each ad-hoc query is a real Flink job
// submission, 10-60+s depending on pool load) - tailing Kafka directly in
// the background is the right way to serve a real-time read model.
//
// The raw/context topics are 'value.format' = 'json-registry' (see
// terraform/flink_statements.tf), so each message's value is
// Confluent-wire-framed (1-byte magic + 4-byte schema ID + JSON payload) -
// decodeValue() below just strips that header, since we don't need the
// schema for reading (only for producing, in confluent-client.js).
//
// If the required environment variables aren't set (see SETUP.md - get
// them with `terraform output` from the terraform/ directory), this
// module falls back to the original in-memory simulator so the MCP server
// still works out of the box without any Confluent Cloud setup.

import { Kafka, logLevel } from 'kafkajs';
import { produceRecord, purgeAllTopics } from './confluent-client.js';

const GEOZONES = [
  'tullamarine-linehaul',
  'docklands-cbd',
  'essendon-fields',
  'westgate-linehaul',
  'dandenong-south',
  'melbourne-cbd',
];

// Real-world anchor coordinates for each geozone - only used by the
// SIMULATED fallback engine below, so its vehicles still plot at coherent
// Melbourne metro positions on the console's map (LIVE mode gets real
// lat/lng straight from vehicle.telemetry's position field instead).
const GEOZONE_COORDS = {
  'melbourne-cbd': [-37.8136, 144.9631],
  'docklands-cbd': [-37.8142, 144.9490],
  'tullamarine-linehaul': [-37.6996, 144.8410],
  'essendon-fields': [-37.7280, 144.9020],
  'westgate-linehaul': [-37.8168, 144.8830],
  'dandenong-south': [-37.9930, 145.2150],
};

const LIVE_ENV = [
  'KAFKA_BOOTSTRAP_ENDPOINT', 'KAFKA_API_KEY', 'KAFKA_API_SECRET',
  'KAFKA_REST_ENDPOINT', 'KAFKA_CLUSTER_ID',
  'SCHEMA_REGISTRY_ENDPOINT', 'SCHEMA_REGISTRY_API_KEY', 'SCHEMA_REGISTRY_API_SECRET',
];

export function isLiveModeConfigured() {
  return LIVE_ENV.every((k) => process.env[k]);
}

function decodeValue(buffer) {
  // byte 0 = magic (0x00), bytes 1-4 = big-endian schema ID, rest = JSON payload
  return JSON.parse(buffer.subarray(5).toString('utf8'));
}

function severityToRisk(severity) {
  if (severity === 'high') return 90;
  if (severity === 'medium') return 65;
  if (severity === 'low') return 30;
  return 0;
}

class LiveContextEngine {
  constructor() {
    this.vehicles = new Map();
    this.weather = new Map();
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    this.deliveryPerformanceEvents = [];
    this._recentMsgTimestamps = [];
    this.ready = false;

    const brokerHost = process.env.KAFKA_BOOTSTRAP_ENDPOINT.replace(/^SASL_SSL:\/\//, '');
    this.kafka = new Kafka({
      clientId: 'fleet-mcp-server',
      brokers: [brokerHost],
      ssl: true,
      sasl: { mechanism: 'plain', username: process.env.KAFKA_API_KEY, password: process.env.KAFKA_API_SECRET },
      connectionTimeout: 10000,
      requestTimeout: 30000,
      retry: { retries: 8, initialRetryTime: 1000, maxRetryTime: 10000 },
      logLevel: logLevel.WARN,
    });

    this._consuming = this._startConsumer();
  }

  async _startConsumer() {
    const consumer = this.kafka.consumer({ groupId: `fleet-mcp-server-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({
      topics: [
        'vehicle.telemetry', 'weather.conditions', 'traffic.incidents', 'parcel.volume',
        'maintenance.alerts', 'driver.risk.events', 'delivery.performance.events',
        'ai.maintenance.recommendations', 'ai.safety.recommendations', 'ai.delivery.recommendations',
      ],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        if (!message.value) return;
        let record;
        try {
          record = decodeValue(message.value);
        } catch {
          return; // skip anything that isn't wire-framed JSON we can decode
        }
        // Real basis for the "events/sec" KPI: every message consumed,
        // across all 10 topics, timestamped and trimmed to a 10s window.
        const now = Date.now();
        this._recentMsgTimestamps.push(now);
        this._recentMsgTimestamps = this._recentMsgTimestamps.filter((t) => now - t < 10000);
        this._applyRecord(topic, record);
      },
    });

    // kafkajs has no built-in "caught up to the point where consumption
    // started" signal for a simple run() loop; a short grace period after
    // connecting is good enough for this demo's fixed-size seed batches.
    setTimeout(() => { this.ready = true; }, 8000);
  }

  _applyRecord(topic, r) {
    switch (topic) {
      case 'vehicle.telemetry': {
        const geozone = r.geozone_ids?.[0];
        const existing = this.vehicles.get(r.object_id);
        if (existing && existing.datetime >= r.datetime) break;
        const tires = r.inputs?.tires;
        const avgTyrePressurePsi = tires?.length
          ? tires.reduce((s, t) => s + (t.tire_pressure || 0), 0) / tires.length
          : existing?.avgTyrePressurePsi;
        this.vehicles.set(r.object_id, {
          ...existing,
          id: r.object_id,
          datetime: r.datetime,
          geozone,
          lat: r.position?.latitude ?? existing?.lat,
          lng: r.position?.longitude ?? existing?.lng,
          ignitionStatus: r.ignition_status,
          tripType: r.trip_type,
          speed: r.position?.speed,
          engineTemp: r.inputs?.calculated_inputs?.temperature,
          fuel: r.inputs?.calculated_inputs?.fuel_level,
          overspeed: r.inputs?.device_inputs?.overspeeding_events === 'OVERSPEED',
          harshBrakingEvents: r.inputs?.device_inputs?.ecodrive_braking_events,
          avgTyrePressurePsi: avgTyrePressurePsi != null ? Number(avgTyrePressurePsi.toFixed(1)) : undefined,
          maintRisk: existing?.maintRisk ?? 0,
          driverRisk: existing?.driverRisk ?? 0,
          parcelsOnboard: existing?.parcelsOnboard ?? 0,
        });
        break;
      }
      case 'weather.conditions': {
        const existing = this.weather.get(r.geozone);
        if (existing && existing.updatedAt >= r.updated_at) break;
        this.weather.set(r.geozone, { geozone: r.geozone, condition: r.condition, tempC: r.temp_c, windKph: r.wind_kph, updatedAt: r.updated_at });
        break;
      }
      case 'traffic.incidents': {
        this.trafficIncidents.unshift({ geozone: r.geozone, type: r.type, severity: r.severity, timestamp: r.timestamp });
        this.trafficIncidents = this.trafficIncidents.slice(0, 50);
        break;
      }
      case 'parcel.volume': {
        this.parcelVolumeLog.unshift({ vehicleId: r.vehicle_id, geozone: r.geozone, parcelsOnboard: r.parcels_onboard, parcelsDeliveredLastHour: r.parcels_delivered_last_hour, timestamp: r.timestamp });
        this.parcelVolumeLog = this.parcelVolumeLog.slice(0, 100);
        const v = this.vehicles.get(r.vehicle_id);
        if (v) v.parcelsOnboard = r.parcels_onboard;
        break;
      }
      case 'maintenance.alerts': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) v.maintRisk = severityToRisk(r.severity);
        break;
      }
      case 'driver.risk.events': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) {
          v.driverRisk = severityToRisk(r.risk_level);
          v.overspeed = r.overspeed_status === 'OVERSPEED';
          v.harshBrakingEvents = r.harsh_braking_score;
        }
        this.driverRiskEvents.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          speedKmh: r.speed_kmh,
          overspeedStatus: r.overspeed_status,
          harshBrakingScore: r.harsh_braking_score,
          riskLevel: r.risk_level,
          eventTime: r.event_time,
          receivedAt: Date.now(),
        });
        this.driverRiskEvents = this.driverRiskEvents.slice(0, 200);
        break;
      }
      case 'delivery.performance.events': {
        const v = this.vehicles.get(r.vehicle_id);
        if (v) {
          v.performanceStatus = r.performance_status;
          v.parcelsDeliveredLastHour = r.parcels_delivered_last_hour;
        }
        this.deliveryPerformanceEvents.unshift({
          vehicleId: r.vehicle_id,
          geozone: r.geozone,
          parcelsOnboard: r.parcels_onboard,
          parcelsDeliveredLastHour: r.parcels_delivered_last_hour,
          performanceStatus: r.performance_status,
          eventTime: r.event_time,
          receivedAt: Date.now(),
        });
        this.deliveryPerformanceEvents = this.deliveryPerformanceEvents.slice(0, 200);
        break;
      }
      case 'ai.maintenance.recommendations':
        this._pushRecommendation('maintenance', r.vehicle_id, r.recommendation, r.severity);
        break;
      case 'ai.safety.recommendations':
        this._pushRecommendation('safety', r.vehicle_id, r.recommendation, r.risk_level);
        break;
      case 'ai.delivery.recommendations':
        this._pushRecommendation('delivery', r.vehicle_id, r.recommendation, r.performance_status);
        break;
      default:
        break;
    }
  }

  _pushRecommendation(domain, vehicleId, text, severity) {
    this.recommendations.unshift({ domain, vehicleId, text, severity, timestamp: new Date().toISOString() });
    this.recommendations = this.recommendations.slice(0, 200);
  }

  // --- public interface matching the original simulator's shape --------

  async getFleetSummary() {
    const list = [...this.vehicles.values()];
    const avg = (fn) => (list.length ? list.reduce((s, v) => s + (fn(v) || 0), 0) / list.length : 0);
    const now = Date.now();
    const recentDriverEvents = (windowMs) => this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const recentDeliveryEvents = this.deliveryPerformanceEvents.slice(0, 30);
    const onTimeCount = recentDeliveryEvents.filter((e) => e.performanceStatus !== 'behind_schedule').length;

    return {
      vehiclesOnline: list.length,
      avgSpeedKmh: Number(avg((v) => v.speed).toFixed(1)),
      avgEngineTempC: Number(avg((v) => v.engineTemp).toFixed(1)),
      avgFuelLevelPct: Number(avg((v) => v.fuel).toFixed(1)),
      avgTyrePressurePsi: Number(avg((v) => v.avgTyrePressurePsi).toFixed(1)),
      totalParcelsOnboard: list.reduce((s, v) => s + (v.parcelsOnboard || 0), 0),
      highDriverRiskCount: list.filter((v) => v.driverRisk > 70).length,
      highMaintenanceRiskCount: list.filter((v) => v.maintRisk > 70).length,
      // "Open work order" = a vehicle whose most recent maintenance.alerts
      // message put it over the risk threshold and nothing since has
      // cleared it. There's no separate open/closed work-order state in
      // this topic's schema, so a vehicle currently over threshold IS the
      // open work order - same underlying number as highMaintenanceRiskCount,
      // exposed under its business name for the console's Maintenance view.
      openWorkOrders: list.filter((v) => v.maintRisk > 70).length,
      activeTrafficIncidents: this.trafficIncidents.length,
      openRecommendations: this.recommendations.length,
      recommendationCounts: {
        safety: this.recommendations.filter((r) => r.domain === 'safety').length,
        delivery: this.recommendations.filter((r) => r.domain === 'delivery').length,
        maintenance: this.recommendations.filter((r) => r.domain === 'maintenance').length,
      },
      // Real measured throughput - count of Kafka messages (any topic)
      // consumed in the trailing 10s window, not a fabricated number.
      eventsPerSecond: Number((this._recentMsgTimestamps.length / 10).toFixed(1)),
      // harsh_braking_score > 0.7 is the same threshold the real Flink job
      // (derive_driver_risk_events) uses to flag a harsh-braking event.
      harshBrakingEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.harshBrakingScore > 0.7).length,
      overspeedEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.overspeedStatus === 'OVERSPEED').length,
      // Replaces the old "Fatigue flags" tile, which had no backing data
      // anywhere in the real schema (no duty-hour/fatigue field exists).
      highRiskEventsRecent: recentDriverEvents(15 * 60 * 1000).filter((e) => e.riskLevel === 'high').length,
      onTimeRatePct: recentDeliveryEvents.length ? Number(((onTimeCount / recentDeliveryEvents.length) * 100).toFixed(0)) : null,
      // Replaces the old "Avg delivery ETA drift" tile, which had no real
      // scheduled/actual-time field to compute drift from.
      avgParcelsDeliveredPerHour: recentDeliveryEvents.length
        ? Number((recentDeliveryEvents.reduce((s, e) => s + (e.parcelsDeliveredLastHour || 0), 0) / recentDeliveryEvents.length).toFixed(1))
        : null,
      routesDisruptedCount: list.filter((v) => v.performanceStatus === 'behind_schedule').length + this.trafficIncidents.length,
      generatedAt: new Date().toISOString(),
      source: 'live-confluent-cloud',
      consumerCaughtUp: this.ready,
    };
  }

  async getVehicle(id) {
    const v = this.vehicles.get(id);
    if (!v) return null;
    return { ...v, currentWeather: this.weather.get(v.geozone) ?? null };
  }

  async listVehicles() {
    return [...this.vehicles.values()].map((v) => ({ ...v, currentWeather: this.weather.get(v.geozone) ?? null }));
  }

  async listHighRisk(domain) {
    const list = await this.listVehicles();
    if (domain === 'safety') return list.filter((v) => v.driverRisk > 70);
    if (domain === 'maintenance') return list.filter((v) => v.maintRisk > 70);
    return list.filter((v) => v.driverRisk > 70 || v.maintRisk > 70);
  }

  async getRecentRecommendations(domain, limit = 10) {
    const filtered = domain ? this.recommendations.filter((r) => r.domain === domain) : this.recommendations;
    return filtered.slice(0, limit);
  }

  async getWeather(geozone) {
    if (geozone) return this.weather.get(geozone) ?? null;
    return [...this.weather.values()];
  }

  async getTrafficIncidents(geozone, limit = 10) {
    const filtered = geozone ? this.trafficIncidents.filter((i) => i.geozone === geozone) : this.trafficIncidents;
    return filtered.slice(0, limit);
  }

  async getParcelVolume(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.parcelVolumeLog.filter((p) => p.vehicleId === vehicleId) : this.parcelVolumeLog;
    return filtered.slice(0, limit);
  }

  // Demo control: produces a REAL wire-encoded record to vehicle.telemetry
  // with a spiked engine temperature, so the real Flink pipeline derives a
  // real maintenance alert and the real Bedrock model generates a real
  // recommendation - the live-data equivalent of the "Inject engine spike"
  // button in the HTML console. Reflects back into this process's own
  // in-memory state once its own consumer reads the message back.
  async injectIncident(vehicleId) {
    const ids = [...this.vehicles.keys()];
    const id = vehicleId ?? ids[Math.floor(Math.random() * ids.length)];
    const base = this.vehicles.get(id);
    const geozone = base?.geozone ?? GEOZONES[Math.floor(Math.random() * GEOZONES.length)];
    const record = {
      object_id: id,
      datetime: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
      ignition_status: 'ON',
      trip_type: 'BUSINESS',
      position: { altitude: 50, longitude: 144.9, latitude: -37.8, direction: 180, satellites_count: 10, speed: 40 },
      inputs: {
        other: { country_code_geonames: 2077456, virtual_gps_odometer: 100000 },
        calculated_inputs: { fuel_consumption: 0.5, fuel_level: 50, mileage: 100000, rpm: 2000, temperature: 104, weight: 2000 },
        device_inputs: {
          priority: 'HIGH', movement: 'MOVING', hdop: '0.5',
          power_supply_voltage: 13, battery_voltage: 12, gps_speed: 40, gsm_signal_strength: 0.8, operator: 0.5,
          engine_rpm: 2000, canbus_engine_coolant_temperature: 104, fuel_level_can: 50, speed_wheel: 40,
          overspeeding_events: 'OVERSPEED', ecodrive_braking_events: 0.9, ecodrive_harsh_acceleration: 0.5, ecodrive_idling_time: 0.1,
          lcv_driver_doors: 'CLOSE', lcv_left_back_doors: 'CLOSE', lcv_right_back_doors: 'CLOSE',
        },
        tires: [1, 2, 3, 4].map((n) => ({
          tire_id: `tire0${n}`, tire_pressure: 95, tire_temperature: 30,
          tire_air_leakage_rate: 0, tire_pressure_threshold_detection: 0, tire_status: 0,
          tire_sensor_electrical_fault: 0, tire_sensor_enable_status: 1, tire_location: n, tire_extended_tire_pressure_support: 1,
        })),
      },
      geozone_ids: [geozone],
    };
    await produceRecord('vehicle.telemetry', record);
    return { id, geozone, injected: 'engine_spike', note: 'Real record produced to vehicle.telemetry - the real Flink pipeline will derive a maintenance alert and Bedrock recommendation within its normal processing latency (usually well under a minute).' };
  }

  // Demo control: produces a REAL high-severity traffic incident record.
  async injectTrafficIncident(geozone) {
    const g = geozone ?? GEOZONES[Math.floor(Math.random() * GEOZONES.length)];
    const record = {
      geozone: g,
      type: 'collision reported',
      severity: 'high',
      timestamp: new Date().toISOString().replace(/\.\d+Z$/, '.000Z'),
    };
    await produceRecord('traffic.incidents', record);
    return { ...record, note: 'Real record produced to traffic.incidents.' };
  }

  // Demo control: HARD reset. Purges all 10 real topics (see
  // confluent-client.js's purgeAllTopics for how - retention-based, not
  // drop/recreate, so the topics' schemas and the 6 persistent Flink jobs
  // are untouched) and clears this process's own in-memory view. This is
  // destructive and irreversible - the real seeded data and every
  // real Bedrock recommendation generated so far are actually deleted.
  async resetPipeline() {
    const result = await purgeAllTopics();
    this.vehicles.clear();
    this.weather.clear();
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    this.deliveryPerformanceEvents = [];
    this._recentMsgTimestamps = [];
    return { ...result, note: 'Real topics purged and in-memory state cleared. The 6 Flink jobs and Bedrock connection were not touched - new data will flow through normally.' };
  }
}

// --- fallback: original in-memory simulator, used only when Confluent
// Cloud environment variables aren't configured, so the server still runs
// out of the box without any setup. ---

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function makeVehicle(i) {
  const geozone = pick(GEOZONES);
  const [homeLat, homeLng] = GEOZONE_COORDS[geozone];
  return {
    id: 'VH-' + String(1000 + i),
    tripType: pick(['BUSINESS', 'UNKNOWN']),
    ignitionStatus: 'ON',
    geozone,
    homeLat, homeLng,
    lat: homeLat + (Math.random() - 0.5) * 0.02,
    lng: homeLng + (Math.random() - 0.5) * 0.02,
    dLat: (Math.random() - 0.5) * 0.0016,
    dLng: (Math.random() - 0.5) * 0.0016,
    speed: 30 + Math.random() * 50,
    engineTemp: 78 + Math.random() * 12,
    fuel: 40 + Math.random() * 50,
    avgTyrePressurePsi: 92 + Math.random() * 10,
    driverRisk: Math.random() * 35,
    maintRisk: Math.random() * 35,
    overspeed: false,
    harshBrakingEvents: 0,
    performanceStatus: 'on_schedule',
    parcelsDeliveredLastHour: Math.round(Math.random() * 10),
    parcelsOnboard: Math.round(20 + Math.random() * 60),
    spiked: false,
    lastUpdated: new Date().toISOString(),
  };
}

function makeWeather(geozone) {
  return {
    geozone,
    condition: pick(['clear', 'clear', 'clear', 'light rain', 'heavy rain', 'fog']),
    tempC: Math.round(10 + Math.random() * 20),
    windKph: Math.round(5 + Math.random() * 35),
    updatedAt: new Date().toISOString(),
  };
}

class SimulatedContextEngine {
  constructor() {
    this.vehicles = new Map(
      Array.from({ length: 12 }, (_, i) => makeVehicle(i)).map((v) => [v.id, v])
    );
    this.weather = new Map(GEOZONES.map((g) => [g, makeWeather(g)]));
    this.trafficIncidents = [];
    this.parcelVolumeLog = [];
    this.recommendations = [];
    this.driverRiskEvents = [];
    this.deliveryPerformanceEvents = [];
    this._recentMsgTimestamps = [];
    this._interval = setInterval(() => this.tick(), 1000);
  }

  _recordEvent() {
    const now = Date.now();
    this._recentMsgTimestamps.push(now);
    this._recentMsgTimestamps = this._recentMsgTimestamps.filter((t) => now - t < 10000);
  }

  stop() { clearInterval(this._interval); }

  tick() {
    if (Math.random() < 0.1) {
      const g = pick(GEOZONES);
      this.weather.set(g, makeWeather(g));
      this._recordEvent();
    }
    if (Math.random() < 0.03) {
      const g = pick(GEOZONES);
      this.trafficIncidents.unshift({
        geozone: g,
        type: pick(['congestion', 'road closure', 'collision reported', 'roadworks']),
        severity: pick(['low', 'medium', 'high']),
        timestamp: new Date().toISOString(),
      });
      this.trafficIncidents = this.trafficIncidents.slice(0, 50);
      this._recordEvent();
    }
    for (const v of this.vehicles.values()) {
      const w = this.weather.get(v.geozone);
      const wetRoad = w.condition === 'heavy rain' || w.condition === 'light rain';
      const activeIncidentHere = this.trafficIncidents.find((i) => i.geozone === v.geozone);

      v.lat += v.dLat; v.lng += v.dLng;
      if (Math.abs(v.lat - v.homeLat) > 0.015) v.dLat *= -1;
      if (Math.abs(v.lng - v.homeLng) > 0.015) v.dLng *= -1;

      v.speed = Math.max(0, v.speed + (Math.random() - 0.5) * 8 - (activeIncidentHere ? 4 : 0));
      if (!v.spiked) v.engineTemp = Math.max(70, Math.min(99, v.engineTemp + (Math.random() - 0.5) * 2));
      v.fuel = Math.max(5, v.fuel - Math.random() * 0.15);
      v.avgTyrePressurePsi = Math.max(80, v.avgTyrePressurePsi + (Math.random() - 0.5) * 0.5);
      v.overspeed = v.speed > 95;

      const harshBrakeChance = wetRoad ? 0.12 : 0.04;
      const harshBrakingScore = Math.random() < harshBrakeChance ? 0.7 + Math.random() * 0.3 : Math.random() * 0.6;
      if (harshBrakingScore > 0.7) v.harshBrakingEvents += 1;

      v.driverRisk = Math.max(0, Math.min(100,
        v.driverRisk
        + (v.overspeed ? Math.random() * 15 : -Math.random() * 3)
        + (wetRoad ? Math.random() * 4 : 0)
      ));
      v.maintRisk = v.engineTemp > 95 ? Math.min(100, v.maintRisk + 6) : Math.max(0, v.maintRisk - 1);
      this._recordEvent();

      // Mirrors the real driver.risk.events schema/thresholds
      // (terraform/flink_statements.tf's derive_driver_risk_events), so
      // getFleetSummary()'s rolling-window stats work identically in both
      // modes.
      const riskLevel = (v.overspeed && harshBrakingScore > 0.7) ? 'high' : (v.overspeed || harshBrakingScore > 0.7) ? 'medium' : 'low';
      this.driverRiskEvents.unshift({
        vehicleId: v.id, geozone: v.geozone, harshBrakingScore,
        overspeedStatus: v.overspeed ? 'OVERSPEED' : 'NO_OVERSPEED',
        riskLevel, receivedAt: Date.now(),
      });
      this.driverRiskEvents = this.driverRiskEvents.slice(0, 200);

      if (Math.random() < 0.15) {
        v.parcelsOnboard = Math.max(0, v.parcelsOnboard + Math.round((Math.random() - 0.6) * 6));
        v.parcelsDeliveredLastHour = Math.round(Math.random() * 20);
        v.performanceStatus = (v.parcelsOnboard > 70 && v.parcelsDeliveredLastHour < 5) ? 'behind_schedule'
          : (v.parcelsDeliveredLastHour >= 15) ? 'ahead_of_schedule' : 'on_schedule';
        this.parcelVolumeLog.unshift({ vehicleId: v.id, geozone: v.geozone, parcelsOnboard: v.parcelsOnboard, timestamp: new Date().toISOString() });
        this.parcelVolumeLog = this.parcelVolumeLog.slice(0, 100);
        this.deliveryPerformanceEvents.unshift({
          vehicleId: v.id, geozone: v.geozone, parcelsOnboard: v.parcelsOnboard,
          parcelsDeliveredLastHour: v.parcelsDeliveredLastHour, performanceStatus: v.performanceStatus,
          receivedAt: Date.now(),
        });
        this.deliveryPerformanceEvents = this.deliveryPerformanceEvents.slice(0, 200);
        this._recordEvent();
      }

      v.lastUpdated = new Date().toISOString();

      if (v.maintRisk > 70 && Math.random() < 0.08) {
        this.pushRecommendation('maintenance', v.id,
          `Vehicle ${v.id} shows signs of cooling-system degradation near ${v.geozone}. Inspect within 48 hours.`,
          v.maintRisk > 85 ? 'high' : 'medium');
      }
      if (v.driverRisk > 70 && Math.random() < 0.06) {
        const weatherNote = wetRoad ? ` (road conditions in ${v.geozone} are currently ${w.condition})` : '';
        this.pushRecommendation('safety', v.id,
          `Driver on ${v.id} flagged for a harsh-braking / overspeed pattern${weatherNote} - recommend a coaching check-in.`,
          v.driverRisk > 85 ? 'high' : 'medium');
      }
      if (activeIncidentHere && Math.random() < 0.1) {
        this.pushRecommendation('delivery', v.id,
          `${v.id} approaching a ${activeIncidentHere.severity}-severity ${activeIncidentHere.type} in ${v.geozone} - recommend rerouting remaining stops.`,
          activeIncidentHere.severity === 'high' ? 'high' : 'medium');
      }
    }
  }

  pushRecommendation(domain, vehicleId, text, severity) {
    this.recommendations.unshift({
      domain, vehicleId, text, severity,
      confidence: Math.round(85 + Math.random() * 10),
      timestamp: new Date().toISOString(),
    });
    this.recommendations = this.recommendations.slice(0, 200);
  }

  async getFleetSummary() {
    const list = [...this.vehicles.values()];
    const avg = (fn) => list.reduce((s, v) => s + fn(v), 0) / list.length;
    const now = Date.now();
    const recentDriverEvents = (windowMs) => this.driverRiskEvents.filter((e) => now - e.receivedAt < windowMs);
    const recentDeliveryEvents = this.deliveryPerformanceEvents.slice(0, 30);
    const onTimeCount = recentDeliveryEvents.filter((e) => e.performanceStatus !== 'behind_schedule').length;

    return {
      vehiclesOnline: list.length,
      avgSpeedKmh: Number(avg((v) => v.speed).toFixed(1)),
      avgEngineTempC: Number(avg((v) => v.engineTemp).toFixed(1)),
      avgTyrePressurePsi: Number(avg((v) => v.avgTyrePressurePsi).toFixed(1)),
      totalParcelsOnboard: list.reduce((s, v) => s + v.parcelsOnboard, 0),
      highDriverRiskCount: list.filter((v) => v.driverRisk > 70).length,
      highMaintenanceRiskCount: list.filter((v) => v.maintRisk > 70).length,
      openWorkOrders: list.filter((v) => v.maintRisk > 70).length,
      activeTrafficIncidents: this.trafficIncidents.length,
      openRecommendations: this.recommendations.length,
      recommendationCounts: {
        safety: this.recommendations.filter((r) => r.domain === 'safety').length,
        delivery: this.recommendations.filter((r) => r.domain === 'delivery').length,
        maintenance: this.recommendations.filter((r) => r.domain === 'maintenance').length,
      },
      eventsPerSecond: Number((this._recentMsgTimestamps.length / 10).toFixed(1)),
      harshBrakingEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.harshBrakingScore > 0.7).length,
      overspeedEventsRecent: recentDriverEvents(5 * 60 * 1000).filter((e) => e.overspeedStatus === 'OVERSPEED').length,
      highRiskEventsRecent: recentDriverEvents(15 * 60 * 1000).filter((e) => e.riskLevel === 'high').length,
      onTimeRatePct: recentDeliveryEvents.length ? Number(((onTimeCount / recentDeliveryEvents.length) * 100).toFixed(0)) : null,
      avgParcelsDeliveredPerHour: recentDeliveryEvents.length
        ? Number((recentDeliveryEvents.reduce((s, e) => s + (e.parcelsDeliveredLastHour || 0), 0) / recentDeliveryEvents.length).toFixed(1))
        : null,
      routesDisruptedCount: list.filter((v) => v.performanceStatus === 'behind_schedule').length + this.trafficIncidents.length,
      generatedAt: new Date().toISOString(),
      source: 'in-memory-simulator',
    };
  }

  async getVehicle(id) {
    const v = this.vehicles.get(id);
    if (!v) return null;
    return { ...v, currentWeather: this.weather.get(v.geozone) };
  }

  async listVehicles() {
    return [...this.vehicles.values()].map((v) => ({ ...v, currentWeather: this.weather.get(v.geozone) }));
  }

  async listHighRisk(domain) {
    const list = [...this.vehicles.values()];
    if (domain === 'safety') return list.filter((v) => v.driverRisk > 70);
    if (domain === 'maintenance') return list.filter((v) => v.maintRisk > 70);
    return list.filter((v) => v.driverRisk > 70 || v.maintRisk > 70);
  }

  async getRecentRecommendations(domain, limit = 10) {
    const filtered = domain ? this.recommendations.filter((r) => r.domain === domain) : this.recommendations;
    return filtered.slice(0, limit);
  }

  async getWeather(geozone) {
    if (geozone) return this.weather.get(geozone) ?? null;
    return [...this.weather.values()];
  }

  async getTrafficIncidents(geozone, limit = 10) {
    const filtered = geozone ? this.trafficIncidents.filter((i) => i.geozone === geozone) : this.trafficIncidents;
    return filtered.slice(0, limit);
  }

  async getParcelVolume(vehicleId, limit = 10) {
    const filtered = vehicleId ? this.parcelVolumeLog.filter((p) => p.vehicleId === vehicleId) : this.parcelVolumeLog;
    return filtered.slice(0, limit);
  }

  async injectIncident(vehicleId) {
    const v = vehicleId ? this.vehicles.get(vehicleId) : [...this.vehicles.values()][Math.floor(Math.random() * this.vehicles.size)];
    if (!v) return null;
    v.spiked = true;
    v.engineTemp = 104;
    v.maintRisk = 92;
    v.driverRisk = Math.max(v.driverRisk, 72);
    this.pushRecommendation('maintenance', v.id, `Vehicle ${v.id} shows signs of cooling-system degradation near ${v.geozone}. Inspect the vehicle within 48 hours.`, 'high');
    this.pushRecommendation('safety', v.id, `Elevated engine heat on ${v.id} correlated with harsh-braking pattern - recommend a driver check-in.`, 'medium');
    this.pushRecommendation('delivery', v.id, `${v.id} flagged for depot diversion in ${v.geozone} - reassigning remaining stops to nearest available vehicle.`, 'high');
    setTimeout(() => { v.spiked = false; }, 15000);
    return { ...v };
  }

  async injectTrafficIncident(geozone) {
    const g = geozone ?? pick(GEOZONES);
    const incident = { geozone: g, type: 'collision reported', severity: 'high', timestamp: new Date().toISOString() };
    this.trafficIncidents.unshift(incident);
    this.trafficIncidents = this.trafficIncidents.slice(0, 50);
    return incident;
  }
}

export const isLive = isLiveModeConfigured();
export const contextEngine = isLive ? new LiveContextEngine() : new SimulatedContextEngine();
export { GEOZONES };
