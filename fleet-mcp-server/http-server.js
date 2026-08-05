// http-server.js
//
// A small JSON API over the same context engine the MCP tools use, so the
// browser-based HTML console (fleet-intelligence-console.html) can show
// real pipeline data too. Browsers can't safely hold Confluent Cloud
// credentials or speak the Kafka wire protocol directly, so this process
// (which already has both, for the MCP server) is the proxy - the browser
// only ever talks to http://localhost:PORT over plain JSON.
//
// Runs in the same Node process as the MCP stdio server, sharing one
// context engine instance/Kafka consumer rather than each surface running
// its own.

import http from 'node:http';
import { contextEngine, isLive } from './context-engine.js';

function withCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res, status, data) {
  withCors(res);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return {};
  }
}

export function startHttpServer(port = process.env.HTTP_PORT || 8787) {
  const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
      withCors(res);
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url, `http://localhost:${port}`);
    try {
      if (req.method === 'GET' && url.pathname === '/api/health') {
        return sendJson(res, 200, { ok: true, mode: isLive ? 'live' : 'simulated' });
      }
      if (req.method === 'GET' && url.pathname === '/api/fleet-summary') {
        return sendJson(res, 200, await contextEngine.getFleetSummary());
      }
      if (req.method === 'GET' && url.pathname === '/api/vehicles') {
        return sendJson(res, 200, await contextEngine.listVehicles());
      }
      if (req.method === 'GET' && url.pathname === '/api/high-risk') {
        const domain = url.searchParams.get('domain') || undefined;
        return sendJson(res, 200, await contextEngine.listHighRisk(domain));
      }
      if (req.method === 'GET' && url.pathname === '/api/recommendations') {
        const domain = url.searchParams.get('domain') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getRecentRecommendations(domain, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/weather') {
        const geozone = url.searchParams.get('geozone') || undefined;
        return sendJson(res, 200, await contextEngine.getWeather(geozone));
      }
      if (req.method === 'GET' && url.pathname === '/api/traffic-incidents') {
        const geozone = url.searchParams.get('geozone') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getTrafficIncidents(geozone, limit));
      }
      if (req.method === 'GET' && url.pathname === '/api/parcel-volume') {
        const vehicleId = url.searchParams.get('vehicleId') || undefined;
        const limit = Number(url.searchParams.get('limit')) || 20;
        return sendJson(res, 200, await contextEngine.getParcelVolume(vehicleId, limit));
      }
      if (req.method === 'POST' && url.pathname === '/api/inject-incident') {
        const { vehicleId } = await readJsonBody(req);
        return sendJson(res, 200, await contextEngine.injectIncident(vehicleId));
      }
      if (req.method === 'POST' && url.pathname === '/api/inject-traffic-incident') {
        const { geozone } = await readJsonBody(req);
        return sendJson(res, 200, await contextEngine.injectTrafficIncident(geozone));
      }
      if (req.method === 'POST' && url.pathname === '/api/reset') {
        if (!isLive) {
          return sendJson(res, 400, { error: 'Not in LIVE mode - nothing to reset. See SETUP.md to enable LIVE mode.' });
        }
        return sendJson(res, 200, await contextEngine.resetPipeline());
      }
      sendJson(res, 404, { error: 'Not found' });
    } catch (err) {
      sendJson(res, 500, { error: err.message });
    }
  });

  server.listen(port, () => {
    console.error(`[fleet-context-engine] HTTP API listening on http://localhost:${port} (for fleet-intelligence-console.html)`);
  });

  return server;
}
