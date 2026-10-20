#!/usr/bin/env bash
# One-time setup on an Ubuntu VM (e.g. Oracle Cloud Always Free). From the repo root:
#   bash deploy/setup-vm.sh
# Copy .env (and the latest fuel_data.db and CSVs) from your computer first; see README.
# Safe to re-run: it reinstalls requirements and restarts the service.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "Copy .env from your computer into $PWD first."; exit 1; }

sudo apt-get update
sudo apt-get install -y python3-venv
command -v tailscale >/dev/null || curl -fsSL https://tailscale.com/install.sh | sh

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

sed -e "s|^User=.*|User=$USER|" -e "s|/home/ubuntu/Fuel-Price|$PWD|g" deploy/fuel-dashboard.service \
  | sudo tee /etc/systemd/system/fuel-dashboard.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable fuel-dashboard
sudo systemctl restart fuel-dashboard

# Prints a login link the first time. The dashboard stays bound to 127.0.0.1;
# only devices signed in to your tailnet can reach it.
sudo tailscale up
sudo tailscale serve --bg 8000
tailscale serve status
