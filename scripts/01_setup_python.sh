#!/usr/bin/env bash
# venv ~/parksense-venv + requirements (idempotent)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d "$HOME/parksense-venv" ] || python3 -m venv "$HOME/parksense-venv"
"$HOME/parksense-venv/bin/pip" install -q --upgrade pip
"$HOME/parksense-venv/bin/pip" install -q -r requirements.txt
source scripts/env.sh
python -c "import pandas, requests, pyproj, happybase, pyspark; print('pandas', pandas.__version__, 'pyproj', pyproj.__version__, 'happybase', happybase.__version__, 'pyspark', pyspark.__version__)"
