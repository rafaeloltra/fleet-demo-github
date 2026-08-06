// index.js
//
// Backend process for the fleet telemetry demo: runs the
// fleet context engine (context-engine.js) and exposes its state as a
// small HTTP API for fleet-intelligence-console.html to poll. RTCE
// (Confluent Cloud's own managed MCP server, see terraform/rtce.tf) is the
// only MCP path into this project's data - this process has no MCP
// surface of its own.
//
// context-engine.js automatically uses the REAL Confluent Cloud + Flink +
// Bedrock pipeline if the required environment variables are set (see
// SETUP.md - get them with `terraform output` from the terraform/
// directory), and falls back to an in-memory simulator otherwise.
//
// Run it:
//   npm install
//   npm start

import { isLive } from './context-engine.js';
import { startHttpServer } from './http-server.js';
import { startTelemetrySimulator } from './telemetry-simulator.js';

console.error(`[fleet-context-engine] mode: ${isLive ? 'LIVE (real Confluent Cloud + Flink + Bedrock)' : 'SIMULATED (in-memory, no Confluent Cloud env vars found)'}`);

startHttpServer();

// In LIVE mode, the real topics are otherwise a one-shot seeded batch -
// this keeps real data flowing continuously so the console has something
// new to show between demo-button clicks. SIMULATED mode already ticks
// its own in-memory state on an interval, so it doesn't need this.
if (isLive) {
  startTelemetrySimulator();
}
