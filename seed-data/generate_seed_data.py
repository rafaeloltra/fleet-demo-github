"""
Generates sample Kafka event data for the 4 raw/context topics, matching
the exact schemas registered in Schema Registry by the Flink `CREATE TABLE`
statements in terraform/flink_statements.tf (vehicle.telemetry's shape is
taken from the project's real sample export, "Download messages as CSV...").

Those topics are `'value.format' = 'json-registry'` (see
terraform/flink_statements.tf for why), which means Flink only accepts
records wire-encoded with Confluent's schema-registry framing (a magic
byte + 4-byte schema ID prefix), not plain JSON text. Piping plain JSON
into `confluent kafka topic produce` will NOT work against these topics -
use --produce below instead, which fetches each topic's schema ID and
wire-encodes correctly.

Usage:
    python3 generate_seed_data.py              # writes out/*.jsonl only
    python3 generate_seed_data.py --produce     # one-shot batch: also produces
                                                 # directly to Confluent Cloud
    python3 generate_seed_data.py --loop        # continuous: produces one fresh
                                                 # record every ~2s, forever
                                                 # (Ctrl+C to stop)

--produce is a one-time batch of 485 backdated-timestamp records - once it
finishes, nothing is producing, so the Flink jobs' watermarks stop advancing
and just report "N minutes since the last message" (this isn't a bug, it's
correctly reporting an idle topic). --loop is what actually keeps data
flowing in real time: each record uses the current timestamp, so watermarks
stay within a couple of seconds of now for as long as it keeps running.

--produce reads connection details from environment variables - get these
with `terraform output` from the terraform/ directory after `apply`:
    KAFKA_REST_ENDPOINT              (terraform output -raw kafka_rest_endpoint)
    KAFKA_CLUSTER_ID                 (terraform output -raw kafka_cluster_id)
    KAFKA_API_KEY                    (terraform output -raw app_manager_kafka_api_key)
    KAFKA_API_SECRET                 (terraform output -raw app_manager_kafka_api_secret)
    SCHEMA_REGISTRY_ENDPOINT         (terraform output -raw schema_registry_rest_endpoint)
    SCHEMA_REGISTRY_API_KEY          (terraform output -raw app_manager_schema_registry_api_key)
    SCHEMA_REGISTRY_API_SECRET       (terraform output -raw app_manager_schema_registry_api_secret)
"""
import base64
import datetime
import json
import os
import random
import struct
import sys
import time
import urllib.error
import urllib.request

random.seed(7)

GEOZONES = ["tullamarine-linehaul", "docklands-cbd", "essendon-fields",
            "westgate-linehaul", "dandenong-south", "melbourne-cbd"]
VEHICLES = [f"VH-{1000+i}" for i in range(12)]

def now_iso(offset_s=0):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset_s)
    return now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def gen_telemetry(n=200):
    rows = []
    for i in range(n):
        vid = random.choice(VEHICLES)
        gz = random.choice(GEOZONES)
        engine_temp = round(random.uniform(75, 108), 1)
        rows.append({
            "object_id": vid,
            "datetime": now_iso(-i * 3),
            "ignition_status": random.choice(["ON", "ON", "ON", "OFF"]),
            "trip_type": random.choice(["BUSINESS", "UNKNOWN"]),
            "position": {
                "altitude": round(random.uniform(0, 120), 2),
                "longitude": round(144.9 + random.uniform(-0.3, 0.3), 6),
                "latitude": round(-37.8 + random.uniform(-0.3, 0.3), 6),
                "direction": round(random.uniform(0, 360), 1),
                "satellites_count": random.randint(6, 14),
                "speed": round(random.uniform(0, 100), 1),
            },
            "inputs": {
                "other": {
                    "country_code_geonames": 2077456,
                    "virtual_gps_odometer": round(random.uniform(1000, 200000), 2),
                },
                "calculated_inputs": {
                    "fuel_consumption": round(random.uniform(0.1, 1.0), 3),
                    "fuel_level": round(random.uniform(5, 100), 1),
                    "mileage": round(random.uniform(1000, 200000), 2),
                    "rpm": round(random.uniform(600, 3000), 0),
                    "temperature": engine_temp,
                    "weight": round(random.uniform(500, 4000), 1),
                },
                "device_inputs": {
                    "priority": random.choice(["LOW", "HIGH"]),
                    "movement": "MOVING",
                    "hdop": str(round(random.uniform(0.3, 2.0), 1)),
                    "power_supply_voltage": round(random.uniform(12, 14.5), 2),
                    "battery_voltage": round(random.uniform(11.5, 12.8), 2),
                    "gps_speed": round(random.uniform(0, 100), 1),
                    "gsm_signal_strength": round(random.uniform(0.2, 1.0), 2),
                    "operator": round(random.uniform(0, 1), 2),
                    "engine_rpm": round(random.uniform(600, 3000), 0),
                    "canbus_engine_coolant_temperature": engine_temp,
                    "fuel_level_can": round(random.uniform(5, 100), 1),
                    "speed_wheel": round(random.uniform(0, 100), 1),
                    "overspeeding_events": "OVERSPEED" if random.random() < 0.15 else "NO_OVERSPEED",
                    "ecodrive_braking_events": round(random.random(), 2),
                    "ecodrive_harsh_acceleration": round(random.random(), 2),
                    "ecodrive_idling_time": round(random.random(), 2),
                    "lcv_driver_doors": random.choice(["OPEN", "CLOSE"]),
                    "lcv_left_back_doors": random.choice(["OPEN", "CLOSE"]),
                    "lcv_right_back_doors": random.choice(["OPEN", "CLOSE"]),
                },
                "tires": [
                    {
                        "tire_id": f"tire0{n}",
                        "tire_pressure": round(random.uniform(85, 100), 1),
                        "tire_temperature": round(random.uniform(20, 45), 1),
                        "tire_air_leakage_rate": round(random.uniform(0, 0.3), 3),
                        "tire_pressure_threshold_detection": random.randint(0, 1),
                        "tire_status": random.randint(0, 2),
                        "tire_sensor_electrical_fault": random.randint(0, 1),
                        "tire_sensor_enable_status": random.randint(0, 1),
                        "tire_location": n,
                        "tire_extended_tire_pressure_support": random.randint(0, 1),
                    }
                    for n in range(1, 5)
                ],
            },
            "geozone_ids": [gz],
        })
    return rows

def gen_weather(n=60):
    rows = []
    for i in range(n):
        gz = random.choice(GEOZONES)
        rows.append({
            "geozone": gz,
            "condition": random.choices(
                ["clear", "light rain", "heavy rain", "fog"], weights=[70, 15, 10, 5])[0],
            "temp_c": round(random.uniform(10, 32), 1),
            "wind_kph": round(random.uniform(5, 40), 1),
            "updated_at": now_iso(-i * 30),
        })
    return rows

def gen_traffic_incidents(n=25):
    rows = []
    for i in range(n):
        rows.append({
            "geozone": random.choice(GEOZONES),
            "type": random.choice(["congestion", "road closure", "collision reported", "roadworks"]),
            "severity": random.choice(["low", "medium", "high"]),
            "timestamp": now_iso(-i * 90),
        })
    return rows

def gen_parcel_volume(n=200):
    rows = []
    for i in range(n):
        rows.append({
            "vehicle_id": random.choice(VEHICLES),
            "geozone": random.choice(GEOZONES),
            "parcels_onboard": random.randint(0, 90),
            "parcels_delivered_last_hour": random.randint(0, 20),
            "timestamp": now_iso(-i * 15),
        })
    return rows

def write_jsonl(rows, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(rows)} records -> {path}")

# --- --produce: wire-encode and produce directly to Confluent Cloud --------

def basic_auth_header(key, secret):
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return f"Basic {token}"

def http_request(url, method="GET", headers=None, body=None):
    req = urllib.request.Request(url, method=method, headers=headers or {})
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data=data, timeout=20) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url} -> {e.code}: {e.read().decode()}") from e

def get_schema_id(sr_endpoint, sr_key, sr_secret, subject):
    url = f"{sr_endpoint}/subjects/{subject}/versions/latest"
    headers = {"Authorization": basic_auth_header(sr_key, sr_secret)}
    return http_request(url, headers=headers)["id"]

def wire_encode(schema_id, record):
    payload = json.dumps(record).encode()
    return base64.b64encode(b"\x00" + struct.pack(">I", schema_id) + payload).decode()

def produce_record(kafka_endpoint, kafka_key, kafka_secret, cluster_id, topic, schema_id, record):
    url = f"{kafka_endpoint}/kafka/v3/clusters/{cluster_id}/topics/{topic}/records"
    headers = {
        "Authorization": basic_auth_header(kafka_key, kafka_secret),
        "Content-Type": "application/json",
    }
    body = {"value": {"type": "BINARY", "data": wire_encode(schema_id, record)}}
    http_request(url, method="POST", headers=headers, body=body)

REQUIRED_ENV = [
    "KAFKA_REST_ENDPOINT", "KAFKA_CLUSTER_ID", "KAFKA_API_KEY", "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_ENDPOINT", "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET",
]

def check_env():
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        print("Get these with `terraform output -raw <name>` from the terraform/ directory - see this file's docstring.")
        sys.exit(1)

def produce_all(datasets):
    check_env()
    kafka_endpoint = os.environ["KAFKA_REST_ENDPOINT"]
    cluster_id = os.environ["KAFKA_CLUSTER_ID"]
    kafka_key = os.environ["KAFKA_API_KEY"]
    kafka_secret = os.environ["KAFKA_API_SECRET"]
    sr_endpoint = os.environ["SCHEMA_REGISTRY_ENDPOINT"]
    sr_key = os.environ["SCHEMA_REGISTRY_API_KEY"]
    sr_secret = os.environ["SCHEMA_REGISTRY_API_SECRET"]

    for topic, rows in datasets:
        schema_id = get_schema_id(sr_endpoint, sr_key, sr_secret, f"{topic}-value")
        for record in rows:
            produce_record(kafka_endpoint, kafka_key, kafka_secret, cluster_id, topic, schema_id, record)
        print(f"produced {len(rows)} records -> {topic} (schema id {schema_id})")

# --- --loop: continuous production, one fresh record every ~2s -------------

GENERATORS = {
    "vehicle.telemetry": lambda: gen_telemetry(1)[0],
    "weather.conditions": lambda: gen_weather(1)[0],
    "traffic.incidents": lambda: gen_traffic_incidents(1)[0],
    "parcel.volume": lambda: gen_parcel_volume(1)[0],
}

def run_loop(interval_s=2.0):
    check_env()
    kafka_endpoint = os.environ["KAFKA_REST_ENDPOINT"]
    cluster_id = os.environ["KAFKA_CLUSTER_ID"]
    kafka_key = os.environ["KAFKA_API_KEY"]
    kafka_secret = os.environ["KAFKA_API_SECRET"]
    sr_endpoint = os.environ["SCHEMA_REGISTRY_ENDPOINT"]
    sr_key = os.environ["SCHEMA_REGISTRY_API_KEY"]
    sr_secret = os.environ["SCHEMA_REGISTRY_API_SECRET"]

    topics = list(GENERATORS.keys())
    schema_ids = {t: get_schema_id(sr_endpoint, sr_key, sr_secret, f"{t}-value") for t in topics}
    print(f"Producing one record every {interval_s}s across {len(topics)} topics. Ctrl+C to stop.")

    count = 0
    try:
        while True:
            topic = random.choice(topics)
            record = GENERATORS[topic]()
            produce_record(kafka_endpoint, kafka_key, kafka_secret, cluster_id, topic, schema_ids[topic], record)
            count += 1
            print(f"[{count}] produced -> {topic}")
            time.sleep(interval_s)
    except KeyboardInterrupt:
        print(f"\nStopped after producing {count} records.")

if __name__ == "__main__":
    if "--loop" in sys.argv:
        run_loop()
    else:
        datasets = [
            ("vehicle.telemetry", gen_telemetry()),
            ("weather.conditions", gen_weather()),
            ("traffic.incidents", gen_traffic_incidents()),
            ("parcel.volume", gen_parcel_volume()),
        ]
        for topic, rows in datasets:
            write_jsonl(rows, f"out/{topic}.jsonl")

        if "--produce" in sys.argv:
            produce_all(datasets)
