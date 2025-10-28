#!/usr/bin/env bash
set -euo pipefail

green(){ printf "\033[32m%s\033[0m\n" "$*"; }
red(){ printf "\033[31m%s\033[0m\n" "$*"; }

usage(){
  cat <<EOF
Constella installer

Usage:
  ./install.sh init  --server-name NAME --public-addr HOST:PORT --owner @User --bot-token TOKEN
  ./install.sh join  --server-name NAME --join "join://HOST:PORT?net=...&token=...&ttl=..."

Notes:
  - После init/join отредактируйте при необходимости .env и запустите:
      docker compose up -d --build
EOF
}

cmd=${1:-""}
shift || true

if [[ "$cmd" == "init" ]]; then
  SERVER_NAME=""
  PUBLIC_ADDR=""
  OWNER=""
  BOT_TOKEN=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --server-name) SERVER_NAME="$2"; shift 2;;
      --public-addr) PUBLIC_ADDR="$2"; shift 2;;
      --owner) OWNER="$2"; shift 2;;
      --bot-token) BOT_TOKEN="$2"; shift 2;;
      *) red "Unknown arg $1"; usage; exit 1;;
    esac
  done
  [[ -z "$SERVER_NAME" || -z "$PUBLIC_ADDR" || -z "$OWNER" || -z "$BOT_TOKEN" ]] && { red "Missing args"; usage; exit 1; }

  NETWORK_ID=$(head -c16 /dev/urandom | xxd -p)
  NETWORK_SECRET=$(head -c32 /dev/urandom | xxd -p)

  cat > .env <<EOF
SERVER_NAME=${SERVER_NAME}
LISTEN_ADDR=0.0.0.0:4747
PUBLIC_ADDR=${PUBLIC_ADDR}

OWNER_USERNAME=${OWNER}
BOT_TOKEN=${BOT_TOKEN}

NETWORK_ID=${NETWORK_ID}
NETWORK_SECRET=${NETWORK_SECRET}

SEED_PEERS=
JOIN_URL=
EOF

  mkdir -p state
  cat > state/network_state.json <<EOF
{
  "network_id": "${NETWORK_ID}",
  "owner_username": "${OWNER}",
  "network_secret": "${NETWORK_SECRET}",
  "peers": []
}
EOF
  green "Init ready. Run: docker compose up -d --build"

elif [[ "$cmd" == "join" ]]; then
  SERVER_NAME=""
  JOIN_URL=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --server-name) SERVER_NAME="$2"; shift 2;;
      --join) JOIN_URL="$2"; shift 2;;
      *) red "Unknown arg $1"; usage; exit 1;;
    esac
  done
  [[ -z "$SERVER_NAME" || -z "$JOIN_URL" ]] && { red "Missing args"; usage; exit 1; }
  cat > .env <<EOF
SERVER_NAME=${SERVER_NAME}
LISTEN_ADDR=0.0.0.0:4747
PUBLIC_ADDR=$(hostname -I | awk '{print $1}'):4747

OWNER_USERNAME=
BOT_TOKEN=

NETWORK_ID=
NETWORK_SECRET=

SEED_PEERS=
JOIN_URL=${JOIN_URL}
EOF
  mkdir -p state
  green "Join ready. Run: docker compose up -d --build"

else
  usage
  exit 1
fi
