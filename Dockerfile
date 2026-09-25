# Runs fleet-context-engine (Kafka consumer + HTTP API) and, from the same
# process, serves fleet-intelligence-console.html + fonts/ - see the
# STATIC_ROOT comment in fleet-context-engine/http-server.js for why both
# live under one process here instead of two.
#
# Build from the repo root (this file's directory):
#   docker build -t fleet-intelligence-console .
#   docker run -p 8080:8080 --env-file <your env file> fleet-intelligence-console
#
# For Cloud Run: gcloud run deploy --source . (see setup-demo.md).

FROM node:20-slim

WORKDIR /app

COPY fleet-context-engine/package*.json ./fleet-context-engine/
RUN cd fleet-context-engine && npm ci --omit=dev

COPY fleet-context-engine ./fleet-context-engine
COPY fleet-intelligence-console.html ./
COPY fonts ./fonts

# Cloud Run sets PORT itself (see http-server.js); 8080 is only the local
# `docker run` default when nothing else is specified.
ENV PORT=8080
EXPOSE 8080

WORKDIR /app/fleet-context-engine
CMD ["node", "index.js"]
