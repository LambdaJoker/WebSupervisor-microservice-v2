#!/bin/bash
# Run http-gateway-service. Auto-detects amd64/arm64 binary.
cd "$(dirname "$0")" || exit 1
arch=$(uname -m)
case "$arch" in
    x86_64|amd64)   bin="http-gateway-service_linux_amd64" ;;
    aarch64|arm64)  bin="http-gateway-service_linux_arm64" ;;
    *) echo "Unsupported architecture: $arch"; exit 1 ;;
esac
chmod +x "$bin" 2>/dev/null
echo "[http-gateway-service] starting ($bin)..."
exec ./"$bin" -config_path=./config.json
