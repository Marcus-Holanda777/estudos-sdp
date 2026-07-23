#!/bin/bash
echo "🚀 Starting JupyterLab Server on port 8888..."
jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --allow-root \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --notebook-dir=/opt/spark/sdp-project
