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
  - После init/join вы можете при необходимости поправить .env вручную
  - Запуск:
      docker compose up -d --build
EOF
}

need_bin(){ command -v "$1" >/dev/null 2>&1 || { red "Missing binary: $1"; exit 1; }; }

cmd=${1:-""}
shift || true

case "$cmd" in
  init)
    SERVER_NAME=""
    PUBLIC_ADDR=""
    OWNER=""
    BOT_TOKEN=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --server-name) SERVER_NAME="$2"; shift 2;;
        --public-addr) PUBLIC_ADDR="$2"; shift 2;;
        --owner)       OWNER="$2"; shift 2;;
        --bot-token)   BOT_TOKEN="$2"; shift 2;;
        *) red "Unknown arg $1"; usage; exit 1;;
      esac
    done
    [[ -z "$SERVER_NAME" || -z "$PUBLIC_ADDR" || -z "$OWNER" || -z "$BOT_TOKEN" ]] && { red "Missing args"; usage; exit 1; }

    need_bin openssl

    NETWORK_ID=$(openssl rand -hex 16)
    NETWORK_SECRET=$(openssl rand -hex 32)

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
    ;;

  join)
    SERVER_NAME=""
    JOIN_URL=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --server-name) SERVER_NAME="$2"; shift 2;;
        --join)        JOIN_URL="$2"; shift 2;;
        *) red "Unknown arg $1"; usage; exit 1;;
      esac
    done
    [[ -z "$SERVER_NAME" || -z "$JOIN_URL" ]] && { red "Missing args"; usage; exit 1; }

    # Пытаемся определить публичный IP; fallback на первый локальный
    PUB_IP=$( (curl -4s --max-time 3 ifconfig.co || true) | tr -d '\n' )
    if [[ -z "$PUB_IP" ]]; then
      PUB_IP=$(hostname -I | awk '{print $1}')
    fi
    PUBLIC_ADDR="${PUB_IP}:4747"

    # Запросим @owner и (необязательно) токен бота
    read -rp "Owner (@username): " OWNER_USERNAME
    OWNER_USERNAME=${OWNER_USERNAME:-}
    read -rp "Bot token (optional, Enter to skip): " BOT_TOKEN
    BOT_TOKEN=${BOT_TOKEN:-}

    cat > .env <<EOF
SERVER_NAME=${SERVER_NAME}
LISTEN_ADDR=0.0.0.0:4747
PUBLIC_ADDR=${PUBLIC_ADDR}

OWNER_USERNAME=${OWNER_USERNAME}
BOT_TOKEN=${BOT_TOKEN}

NETWORK_ID=
NETWORK_SECRET=

SEED_PEERS=
JOIN_URL=${JOIN_URL}
EOF

    mkdir -p state
    # пустой минимальный стейт — do_join_if_needed() заполнит после успешного /join
    if [[ ! -f state/network_state.json ]]; then
      cat > state/network_state.json <<EOF
{
  "network_id": "",
  "owner_username": "${OWNER_USERNAME}",
  "network_secret": "",
  "peers": []
}
EOF
    fi

    green "Join ready. Run: docker compose up -d --build"
    ;;

  *)
    usage
    exit 1
    ;;
esac
