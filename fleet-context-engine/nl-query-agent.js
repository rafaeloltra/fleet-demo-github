// nl-query-agent.js
//
// Answers a natural-language question about the fleet by giving OpenAI
// tool-calling access to RTCE's own three tools (rtce-client.js) - the same
// listTopics/getMetadata/queryData surface an MCP client like Claude Code
// would call, just driven by our own backend's model instead of an MCP
// client. Every answer is grounded in a real queryData SQL call against the
// live topics; nothing here is cached or precomputed.
//
// Configured via OPENAI_API_KEY (same account as the Flink OpenAI
// connection, but this call happens in this Node process, not in Flink -
// see terraform/flink.tf for the other one). Optional OPENAI_NL_MODEL
// overrides the model (defaults to gpt-4o-mini).

import { listTopics, getMetadata, queryData } from './rtce-client.js';

const TOOLS = [
  {
    type: 'function',
    function: {
      name: 'listTopics',
      description: 'Returns the list of available topics (tables) in the real-time data store. Call this first if you don\'t already know the topic names.',
      parameters: { type: 'object', properties: {}, additionalProperties: false },
    },
  },
  {
    type: 'function',
    function: {
      name: 'getMetadata',
      description: 'Returns the schema for a topic - column names, types, and nested structure. Call this before queryData on any topic you have not already inspected - never guess column names.',
      parameters: {
        type: 'object',
        properties: { topic_name: { type: 'string', description: 'the name of the topic to get metadata for' } },
        required: ['topic_name'],
        additionalProperties: false,
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'queryData',
      description: 'Run a real SQL SELECT query against a topic in the live Kafka pipeline. Requires calling getMetadata first. The query MUST include a FROM clause naming the exact topic in double quotes, e.g. FROM "vehicle.telemetry" - omitting FROM fails validation. Supports SELECT/WHERE/ORDER BY/LIMIT with standard predicates (=, <>, <, >, BETWEEN, IN, LIKE, IS NULL, AND/OR/NOT). Does NOT support JOINs, aggregate functions (AVG/SUM/COUNT/MIN/MAX), GROUP BY, or subqueries - if the question needs one of those, pull the raw rows you can and compute it yourself, and say so in your answer.',
      parameters: {
        type: 'object',
        properties: {
          topic_name: { type: 'string', description: 'the name of the topic to query' },
          query: { type: 'string', description: 'SQL SELECT query string; must include FROM "<topic_name>" (double-quoted, exact name); double-quote column names too, using the exact casing returned by getMetadata' },
          max_result_rows: { type: 'integer', description: 'maximum rows to return (max 200)' },
        },
        required: ['topic_name', 'query', 'max_result_rows'],
        additionalProperties: false,
      },
    },
  },
];

const TOOL_IMPLS = {
  listTopics: () => listTopics(),
  getMetadata: ({ topic_name }) => getMetadata(topic_name),
  queryData: ({ topic_name, query, max_result_rows }) => queryData(topic_name, query, max_result_rows),
};

const SYSTEM_PROMPT = `You are a chat assistant answering questions about a live fleet-telemetry Kafka pipeline (vehicle telemetry, weather, traffic incidents, parcel volumes, and AI-generated safety/delivery/maintenance recommendations) by querying Confluent's Real-Time Context Engine directly - a read-only SQL interface over the real, currently-flowing Kafka topics. There is no cached or batch copy of this data; every answer must come from an actual queryData call you just made.

Always call listTopics first if you don't already know the topic names, then getMetadata on a topic before querying it. queryData only supports SELECT/WHERE/ORDER BY/LIMIT (no JOINs, aggregates, GROUP BY, or subqueries) - if the question needs one of those, pull the raw rows and compute the aggregate yourself, and say so.

This is a chat UI, not a report generator. Hard rules for every answer, no exceptions:
- 1-3 short sentences, plain conversational prose only.
- No markdown: no **bold**, no headers, no numbered or bulleted lists, no colons introducing a list.
- Never cover more than one topic/domain per answer. If a question is broad ("what's going on", "give me an overview"), pick the single most notable real thing you find (e.g. one vehicle, one alert, one incident) and answer about that specifically - do not survey multiple topics in one reply.
- Cite the actual values returned (vehicle IDs, numbers, timestamps). Do not fabricate data. If a query comes back empty, say so plainly rather than guessing.`;

export async function askAboutFleetData(conversation, { maxSteps = 6 } = {}) {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error('Missing OPENAI_API_KEY environment variable. See SETUP.md.');
  }
  if (!Array.isArray(conversation) || !conversation.length) {
    throw new Error('askAboutFleetData requires a non-empty conversation array.');
  }

  // Cap how much prior chat history gets resent each turn, so a long demo
  // session doesn't grow the request unbounded.
  const recentConversation = conversation.slice(-12);

  const messages = [
    { role: 'system', content: SYSTEM_PROMPT },
    ...recentConversation,
  ];
  const trace = [];

  for (let step = 0; step < maxSteps; step++) {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: process.env.OPENAI_NL_MODEL || 'gpt-4o-mini',
        messages,
        tools: TOOLS,
      }),
    });
    if (!resp.ok) throw new Error(`OpenAI request failed: ${resp.status} ${await resp.text()}`);
    const data = await resp.json();
    const msg = data.choices[0].message;
    messages.push(msg);

    if (!msg.tool_calls || !msg.tool_calls.length) {
      return { answer: msg.content, trace };
    }

    for (const call of msg.tool_calls) {
      const args = JSON.parse(call.function.arguments || '{}');
      let result;
      try {
        result = await TOOL_IMPLS[call.function.name](args);
      } catch (err) {
        result = { error: err.message };
      }
      trace.push({ tool: call.function.name, arguments: args, result });
      messages.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(result) });
    }
  }

  return { answer: "I wasn't able to find a confident answer within the allotted number of steps.", trace };
}
