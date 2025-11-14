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

VPN overlay (WireGuard) options (optional):
  --vpn-mode MODE                 none (default), hub (init) or client (join)
  --vpn-interface IFACE           wg0 by default
  --vpn-cidr CIDR                 private subnet, default 10.42.0.0/24
  --vpn-address ADDRESS/CIDR      hub/client interface address (auto for hub)
  --vpn-listen-port PORT          hub listen port, default 51820
  --vpn-hub-endpoint HOST:PORT    hub endpoint for clients
  --vpn-hub-public-key KEY        hub WireGuard public key (client mode)
  --vpn-allowed-ips CIDR          AllowedIPs for client peers (defaults to VPN CIDR)
  --vpn-preshared-key KEY         optional preshared key for client peers
EOF
}

need_bin(){ command -v "$1" >/dev/null 2>&1 || { red "Missing binary: $1"; exit 1; }; }

ROOT_DIR=$(pwd)
STATE_DIR="state"
WG_DIR="${ROOT_DIR}/${STATE_DIR}/wg"

run_priv(){
  if [[ $EUID -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo "$@"
    else
      return 1
    fi
  else
    "$@"
  fi
}

wait_for_iface_ip(){
  local iface="$1"
  local expected="$2"
  [[ -z "$expected" ]] && return 0
  local addr="${expected%/*}"
  local prefix="${expected#*/}"
  local attempts=20
  while (( attempts > 0 )); do
    if ip -4 -o addr show dev "$iface" 2>/dev/null | awk '{print $4}' | grep -Fx "${addr}/${prefix}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    attempts=$((attempts - 1))
  done
  return 1
}

vpn_generate_keys(){
  local iface="$1"
  mkdir -p "$WG_DIR"
  local priv_file="${WG_DIR}/${iface}.key"
  local pub_file="${WG_DIR}/${iface}.pub"
  local old_umask
  old_umask=$(umask)
  umask 077
  if [[ ! -f "$priv_file" ]]; then
    wg genkey > "$priv_file"
  fi
  umask "$old_umask"
  chmod 600 "$priv_file"
  wg pubkey < "$priv_file" > "$pub_file"
  chmod 600 "$pub_file"
  VPN_PRIVATE_KEY=$(<"$priv_file")
  VPN_PUBLIC_KEY=$(<"$pub_file")
}

vpn_up(){
  local conf_path="$1"
  local iface="$2"
  local expected="$3"
  run_priv wg-quick down "$conf_path" >/dev/null 2>&1 || true
  if ! run_priv wg-quick up "$conf_path"; then
    return 1
  fi
  if [[ -n "$expected" ]] && ! wait_for_iface_ip "$iface" "$expected"; then
    return 2
  fi
  return 0
}

cmd=${1:-""}
shift || true

case "$cmd" in
  init)
    SERVER_NAME=""
    PUBLIC_ADDR=""
    OWNER=""
    BOT_TOKEN=""
    VPN_MODE="none"
    VPN_INTERFACE="wg0"
    VPN_CIDR="10.42.0.0/24"
    VPN_ADDRESS=""
    VPN_LISTEN_PORT="51820"
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --server-name) SERVER_NAME="$2"; shift 2;;
        --public-addr) PUBLIC_ADDR="$2"; shift 2;;
        --owner)       OWNER="$2"; shift 2;;
        --bot-token)   BOT_TOKEN="$2"; shift 2;;
        --vpn-mode)        VPN_MODE="$2"; shift 2;;
        --vpn-interface)   VPN_INTERFACE="$2"; shift 2;;
        --vpn-cidr)        VPN_CIDR="$2"; shift 2;;
        --vpn-address)     VPN_ADDRESS="$2"; shift 2;;
        --vpn-listen-port) VPN_LISTEN_PORT="$2"; shift 2;;
        *) red "Unknown arg $1"; usage; exit 1;;
      esac
    done
    [[ -z "$SERVER_NAME" || -z "$PUBLIC_ADDR" || -z "$OWNER" || -z "$BOT_TOKEN" ]] && { red "Missing args"; usage; exit 1; }

    need_bin openssl

    if [[ "$VPN_MODE" == "hub" ]]; then
      need_bin wg
      need_bin wg-quick
      need_bin ip
      need_bin python3
      if [[ -z "$VPN_ADDRESS" ]]; then
        VPN_ADDRESS=$(python3 - <<PY
import ipaddress
net = ipaddress.ip_network("${VPN_CIDR}", strict=False)
hosts = list(net.hosts())
addr = hosts[0] if hosts else net.network_address
print(f"{addr}/{net.prefixlen}")
PY
)
      fi
    elif [[ "$VPN_MODE" != "none" ]]; then
      red "init supports --vpn-mode hub or none"
      exit 1
    fi

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

VPN_MODE=${VPN_MODE}
VPN_INTERFACE_NAME=${VPN_INTERFACE}
VPN_CIDR=${VPN_CIDR}
VPN_LISTEN_PORT=${VPN_LISTEN_PORT}
VPN_HUB_ENDPOINT=
VPN_ADDRESS=${VPN_ADDRESS}
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

    if [[ "$VPN_MODE" == "hub" ]]; then
      vpn_generate_keys "$VPN_INTERFACE"
      WG_CONF_FILE="${WG_DIR}/${VPN_INTERFACE}.conf"
      mkdir -p "${WG_DIR}"
      cat > "$WG_CONF_FILE" <<EOF
[Interface]
PrivateKey = ${VPN_PRIVATE_KEY}
Address = ${VPN_ADDRESS}
ListenPort = ${VPN_LISTEN_PORT}
SaveConfig = false
# Add [Peer] entries for each client below.
EOF
      chmod 600 "$WG_CONF_FILE"
      WG_CONF_DISPLAY="${STATE_DIR}/wg/${VPN_INTERFACE}.conf"
      green "WireGuard hub config written to ${WG_CONF_DISPLAY}"
      green "Hub public key: ${VPN_PUBLIC_KEY}"
      if vpn_up "$WG_CONF_FILE" "$VPN_INTERFACE" "$VPN_ADDRESS"; then
        green "WireGuard interface ${VPN_INTERFACE} ready at ${VPN_ADDRESS}"
      else
        status=$?
        if [[ $status -eq 2 ]]; then
          red "WireGuard ${VPN_INTERFACE} up but ${VPN_ADDRESS} not detected. Check state/wg configuration."
        else
          red "Failed to start WireGuard automatically. Run 'wg-quick up ${WG_CONF_DISPLAY}' as root after configuring peers."
        fi
      fi
      green "Share the hub public key and allocate client IPs from ${VPN_CIDR} for NAT clients."
    fi

    green "Init ready. Run: docker compose up -d --build"
    ;;

  join)
    SERVER_NAME=""
    JOIN_URL=""
    VPN_MODE="none"
    VPN_INTERFACE="wg0"
    VPN_CIDR="10.42.0.0/24"
    VPN_ADDRESS=""
    VPN_HUB_ENDPOINT=""
    VPN_HUB_PUBLIC_KEY=""
    VPN_ALLOWED_IPS=""
    VPN_PRESHARED_KEY=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --server-name) SERVER_NAME="$2"; shift 2;;
        --join)        JOIN_URL="$2"; shift 2;;
        --vpn-mode)            VPN_MODE="$2"; shift 2;;
        --vpn-interface)       VPN_INTERFACE="$2"; shift 2;;
        --vpn-cidr)            VPN_CIDR="$2"; shift 2;;
        --vpn-address)         VPN_ADDRESS="$2"; shift 2;;
        --vpn-hub-endpoint)    VPN_HUB_ENDPOINT="$2"; shift 2;;
        --vpn-hub-public-key)  VPN_HUB_PUBLIC_KEY="$2"; shift 2;;
        --vpn-allowed-ips)     VPN_ALLOWED_IPS="$2"; shift 2;;
        --vpn-preshared-key)   VPN_PRESHARED_KEY="$2"; shift 2;;
        *) red "Unknown arg $1"; usage; exit 1;;
      esac
    done
    [[ -z "$SERVER_NAME" || -z "$JOIN_URL" ]] && { red "Missing args"; usage; exit 1; }

    if [[ "$VPN_MODE" == "client" ]]; then
      need_bin wg
      need_bin wg-quick
      need_bin ip
      if [[ -z "$VPN_ADDRESS" || -z "$VPN_HUB_ENDPOINT" || -z "$VPN_HUB_PUBLIC_KEY" ]]; then
        red "Client mode requires --vpn-address, --vpn-hub-endpoint and --vpn-hub-public-key"
        exit 1
      fi
      VPN_ALLOWED_IPS=${VPN_ALLOWED_IPS:-$VPN_CIDR}
    elif [[ "$VPN_MODE" != "none" ]]; then
      red "join supports --vpn-mode client or none"
      exit 1
    fi

    # Пытаемся определить публичный IP; fallback на первый локальный
    PUB_IP=$( (curl -4s --max-time 3 ifconfig.co || true) | tr -d '\n' )
    if [[ -z "$PUB_IP" ]]; then
      PUB_IP=$(hostname -I | awk '{print $1}')
    fi
    PUBLIC_ADDR_VALUE="${PUB_IP}:4747"
    PUBLIC_ADDR="${PUBLIC_ADDR_VALUE}"
    if [[ "$VPN_MODE" == "client" ]]; then
      PUBLIC_ADDR=""
    fi

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

VPN_MODE=${VPN_MODE}
VPN_INTERFACE_NAME=${VPN_INTERFACE}
VPN_CIDR=${VPN_CIDR}
VPN_LISTEN_PORT=
VPN_HUB_ENDPOINT=${VPN_HUB_ENDPOINT}
VPN_ADDRESS=${VPN_ADDRESS}
VPN_ALLOWED_IPS=${VPN_ALLOWED_IPS}
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

    if [[ "$VPN_MODE" == "client" ]]; then
      vpn_generate_keys "$VPN_INTERFACE"
      WG_CONF_FILE="${WG_DIR}/${VPN_INTERFACE}.conf"
      mkdir -p "${WG_DIR}"
      {
        echo "[Interface]"
        echo "PrivateKey = ${VPN_PRIVATE_KEY}"
        echo "Address = ${VPN_ADDRESS}"
        echo "SaveConfig = false"
        echo
        echo "[Peer]"
        echo "PublicKey = ${VPN_HUB_PUBLIC_KEY}"
        echo "AllowedIPs = ${VPN_ALLOWED_IPS}"
        echo "Endpoint = ${VPN_HUB_ENDPOINT}"
        echo "PersistentKeepalive = 25"
        if [[ -n "$VPN_PRESHARED_KEY" ]]; then
          echo "PresharedKey = ${VPN_PRESHARED_KEY}"
        fi
      } > "$WG_CONF_FILE"
      chmod 600 "$WG_CONF_FILE"
      WG_CONF_DISPLAY="${STATE_DIR}/wg/${VPN_INTERFACE}.conf"
      green "WireGuard client config written to ${WG_CONF_DISPLAY}"
      green "Client public key: ${VPN_PUBLIC_KEY}"
      green "Provide this public key to the hub and ensure it is added as a peer before bringing the tunnel up."
      if vpn_up "$WG_CONF_FILE" "$VPN_INTERFACE" "$VPN_ADDRESS"; then
        green "WireGuard interface ${VPN_INTERFACE} ready at ${VPN_ADDRESS}"
      else
        status=$?
        if [[ $status -eq 2 ]]; then
          red "WireGuard ${VPN_INTERFACE} up but ${VPN_ADDRESS} not detected. Verify the assigned address."
        else
          red "Failed to start WireGuard automatically. After the hub adds this peer, run 'wg-quick up ${WG_CONF_DISPLAY}' as root."
        fi
      fi
    fi

    green "Join ready. Run: docker compose up -d --build"
    ;;

  *)
    usage
    exit 1
    ;;
esac
