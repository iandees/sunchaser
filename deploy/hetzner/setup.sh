#!/usr/bin/env bash
# Prepare a fresh Ubuntu 24.04 box (run as root, idempotent): packages, uv, Rust, the repository
# in /data/sunchaser, the extractor build, the Python environment, an open-files limit, and
# /etc/sunchaser.env from the template (fill it in afterwards).
set -euo pipefail
REPO=${REPO:-https://github.com/iandees/sunchaser.git}
DIR=${DIR:-/data/sunchaser}

[ -d /data ] || { echo "/data is missing: install with deploy/hetzner/installimage.conf (RAID0, /data partition)"; exit 1; }
apt-get update -qq
apt-get install -y -qq build-essential git curl tmux jq python3 >/dev/null

command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
command -v cargo >/dev/null || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

if [ -d "$DIR/.git" ]; then git -C "$DIR" pull --ff-only; else git clone "$REPO" "$DIR"; fi
cd "$DIR"
cargo build --release --manifest-path rust/osm-extract/Cargo.toml
uv sync --locked

# the extractor keeps one open file per bucket (4 tables x 2^bucket-bits)
cat > /etc/security/limits.d/90-sunchaser.conf <<'LIM'
*    soft nofile 65536
*    hard nofile 65536
root soft nofile 65536
root hard nofile 65536
LIM

if [ ! -f /etc/sunchaser.env ]; then
  install -m 600 deploy/hetzner/sunchaser.env.example /etc/sunchaser.env
  echo "edit /etc/sunchaser.env (R2 endpoint, keys, LAKE) before building"
fi
mkdir -p /data/updater /data/logs
echo "setup done: $(nproc) cores, $(free -g | awk '/Mem/{print $2}') GB RAM, $(df -h /data | awk 'NR==2{print $4}') free on /data"
