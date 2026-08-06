// confluent-client.js
//
// Produces wire-encoded records (Confluent's magic-byte + schema-id
// framing, since the raw/context topics use 'value.format' = 'json-registry'
// - see terraform/flink_statements.tf) to the real Confluent Cloud
// pipeline, for the demo-injection HTTP endpoints (/api/inject-incident /
// /api/inject-traffic-incident). Reading real data is handled separately
// in context-engine.js via a background Kafka consumer (kafkajs) - ad-hoc
// Flink SQL pull queries were tried first for reads and dropped: each one
// is a real Flink job submission that takes 10-60+s and competes with the
// pipeline's own persistent jobs for compute pool capacity, which doesn't
// suit an interactive demo.
//
// Configured via environment variables - get these with `terraform
// output` from the terraform/ directory. See SETUP.md.

const KAFKA_ENV = ['KAFKA_REST_ENDPOINT', 'KAFKA_CLUSTER_ID', 'KAFKA_API_KEY', 'KAFKA_API_SECRET'];
const SR_ENV = ['SCHEMA_REGISTRY_ENDPOINT', 'SCHEMA_REGISTRY_API_KEY', 'SCHEMA_REGISTRY_API_SECRET'];

function assertEnv(keys) {
  const missing = keys.filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`Missing environment variables: ${missing.join(', ')}. See SETUP.md.`);
  }
}

function basicAuth(key, secret) {
  return `Basic ${Buffer.from(`${key}:${secret}`).toString('base64')}`;
}

async function fetchWithRetry(url, options = {}, retries = 3, delayMs = 2000) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetch(url, options);
    } catch (err) {
      lastErr = err;
      if (attempt < retries) await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw lastErr;
}

const schemaIdCache = new Map();

async function getSchemaId(topic) {
  if (schemaIdCache.has(topic)) return schemaIdCache.get(topic);
  assertEnv(SR_ENV);
  const url = `${process.env.SCHEMA_REGISTRY_ENDPOINT}/subjects/${topic}-value/versions/latest`;
  const headers = { Authorization: basicAuth(process.env.SCHEMA_REGISTRY_API_KEY, process.env.SCHEMA_REGISTRY_API_SECRET) };
  const resp = await fetchWithRetry(url, { headers });
  if (!resp.ok) throw new Error(`Failed to look up schema for ${topic}: ${resp.status} ${await resp.text()}`);
  const { id } = await resp.json();
  schemaIdCache.set(topic, id);
  return id;
}

const ALL_TOPICS = [
  'vehicle.telemetry', 'weather.conditions', 'traffic.incidents', 'parcel.volume',
  'maintenance.alerts', 'driver.risk.events', 'delivery.performance.events',
  'ai.maintenance.recommendations', 'ai.safety.recommendations', 'ai.delivery.recommendations',
];

function topicConfigUrl(topic, config) {
  return `${process.env.KAFKA_REST_ENDPOINT}/kafka/v3/clusters/${process.env.KAFKA_CLUSTER_ID}/topics/${topic}/configs/${config}`;
}

async function getTopicConfig(topic, config, headers) {
  const resp = await fetchWithRetry(topicConfigUrl(topic, config), { headers });
  if (!resp.ok) throw new Error(`Failed to read ${config} for ${topic}: ${resp.status} ${await resp.text()}`);
  return (await resp.json()).value;
}

async function setTopicConfig(topic, config, value, headers) {
  const resp = await fetchWithRetry(topicConfigUrl(topic, config), {
    method: 'PUT',
    headers: { ...headers, 'Content-Type': 'application/json' },
    body: JSON.stringify({ value: String(value) }),
  });
  if (!resp.ok) throw new Error(`Failed to set ${config}=${value} for ${topic}: ${resp.status} ${await resp.text()}`);
}

// Purges a topic's messages by briefly dropping its retention window to
// force Kafka's log cleaner to delete existing segments, then restoring
// the original retention. This deletes real data but leaves the topic
// itself - and its registered schema/'json-registry' format - completely
// untouched, so none of the 6 persistent Flink jobs need restarting and
// Terraform's state sees no drift (unlike dropping/recreating the topic,
// which would lose the Flink-registered schema binding entirely - see the
// long comment in terraform/flink_statements.tf for why that's fragile).
async function purgeTopic(topic, headers) {
  const original = await getTopicConfig(topic, 'retention.ms', headers);
  await setTopicConfig(topic, 'retention.ms', '100', headers);
  await new Promise((r) => setTimeout(r, 15000)); // let the log cleaner actually run
  await setTopicConfig(topic, 'retention.ms', original, headers);
}

// Hard-resets the real pipeline: purges all 10 topics in parallel. Does
// NOT touch Flink statements, the Bedrock model/connection, or any
// Terraform-managed resource - only the messages inside the topics.
export async function purgeAllTopics() {
  assertEnv(KAFKA_ENV);
  const headers = { Authorization: basicAuth(process.env.KAFKA_API_KEY, process.env.KAFKA_API_SECRET) };
  const results = await Promise.allSettled(ALL_TOPICS.map((t) => purgeTopic(t, headers)));
  const failed = results
    .map((r, i) => ({ topic: ALL_TOPICS[i], r }))
    .filter(({ r }) => r.status === 'rejected')
    .map(({ topic, r }) => ({ topic, error: r.reason.message }));
  return { purged: ALL_TOPICS.length - failed.length, total: ALL_TOPICS.length, failed };
}

// Wire-encodes a record with Confluent's schema-registry framing (magic
// byte 0x00 + 4-byte big-endian schema ID + JSON payload) and produces it
// to a real topic - the format the json-registry topics require. Mirrors
// seed-data/generate_seed_data.py's Python implementation of the same thing.
export async function produceRecord(topic, record) {
  assertEnv(KAFKA_ENV);
  const schemaId = await getSchemaId(topic);
  const payload = Buffer.from(JSON.stringify(record));
  const header = Buffer.alloc(5);
  header.writeUInt8(0, 0);
  header.writeUInt32BE(schemaId, 1);
  const wireBytes = Buffer.concat([header, payload]);

  const url = `${process.env.KAFKA_REST_ENDPOINT}/kafka/v3/clusters/${process.env.KAFKA_CLUSTER_ID}/topics/${topic}/records`;
  const headers = {
    Authorization: basicAuth(process.env.KAFKA_API_KEY, process.env.KAFKA_API_SECRET),
    'Content-Type': 'application/json',
  };
  const resp = await fetchWithRetry(url, {
    method: 'POST',
    headers,
    body: JSON.stringify({ value: { type: 'BINARY', data: wireBytes.toString('base64') } }),
  });
  if (!resp.ok) throw new Error(`Failed to produce to ${topic}: ${resp.status} ${await resp.text()}`);
  return resp.json();
}
