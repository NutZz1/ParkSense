#!/usr/bin/env bash
# Installs and builds the UI: Express server deps + React client (ui/client/dist, served by the server on :5000).
set -euo pipefail
cd "$(dirname "$0")/.." && source scripts/env.sh
(cd ui/server && npm install --no-audit --no-fund)
cp -f outputs/figures/fig01_architecture.png ui/client/public/ 2>/dev/null || true
(cd ui/client && npm install --no-audit --no-fund && npm run build)
echo "UI built -> ui/client/dist (start with scripts/start_live.sh ui -> http://localhost:5000)"
