#!/usr/bin/env python3
"""Constella – distributed monitoring & control node."""

# --- Imports & global constants ------------------------------------------------

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import signal
import sys
import time
import uuid
from collections import deque
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import psutil
from aiohttp import ClientSession, ClientTimeout, web
from aiogram.exceptions import TelegramBadRequest

# --- Configuration from environment ------------------------------------------------

APP_NAME = "Constella"
STATE_DIR = os.environ.get("STATE_DIR", "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "network_state.json")
INVITES_FILE = os.path.join(STATE_DIR, "invites.json")

SERVER_NAME = os.environ.get("SERVER_NAME", f"node-{uuid.uuid4().hex[:6]}")
LISTEN_ADDR = os.environ.get("LISTEN_ADDR", "0.0.0.0:4747")
PUBLIC_ADDR = os.environ.get("PUBLIC_ADDR", None)  # host:port обязательно при init
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "")  # @username (устанавливается при init)
JOIN_URL = os.environ.get("JOIN_URL", "")  # используется при первом старте join
SEED_PEERS = [p.strip() for p in os.environ.get("SEED_PEERS", "").split(",") if p.strip()]

SAMPLE_EVERY_SEC = int(os.environ.get("SAMPLE_EVERY_SEC", "300"))  # 5 мин
METRICS_WINDOW_H = int(os.environ.get("METRICS_WINDOW_H", "6"))    # последние 6 часов
ENABLE_BG_SPEEDTEST = os.environ.get("ENABLE_BG_SPEEDTEST", "1") == "1"

# Тайм-серии (только в RAM на узле)
_MAX_POINTS = (METRICS_WINDOW_H * 3600) // SAMPLE_EVERY_SEC + 4
CPU_SAMPLES = deque(maxlen=_MAX_POINTS)           # [(ts, cpu_pct)]
NET_DOWN_SAMPLES = deque(maxlen=_MAX_POINTS)      # [(ts, mbps)]
NET_UP_SAMPLES = deque(maxlen=_MAX_POINTS)        # [(ts, mbps)]
SPEEDTEST_LOCK = asyncio.Lock()
METRICS_SUMMARY_POINTS = int(os.environ.get("METRICS_SUMMARY_POINTS", "12"))

# Секрет сети (для HMAC подписи). В init задаётся; при join — приходит от seed.
NETWORK_ID = os.environ.get("NETWORK_ID", "")
NETWORK_SECRET = os.environ.get("NETWORK_SECRET", "")

# Логи
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("constella")
bot_logger = logging.getLogger("constella.bot")
rpc_logger = logging.getLogger("constella.rpc")

# Тайминги
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "2.0"))
DOWN_AFTER_MISSES = int(os.environ.get("DOWN_AFTER_MISSES", "3"))
HEARTBEAT_TIMEOUT = float(os.environ.get(
    "HEARTBEAT_TIMEOUT",
    str(max(1, DOWN_AFTER_MISSES) * HEARTBEAT_INTERVAL),
))
RPC_TIMEOUT = float(os.environ.get("RPC_TIMEOUT", "3.0"))
SPEEDTEST_RPC_TIMEOUT = float(os.environ.get("SPEEDTEST_RPC_TIMEOUT", "45.0"))
CLOCK_SKEW = int(os.environ.get("CLOCK_SKEW", "15"))  # сек, допускаемая рассинхронизация в RPC

LEADER_GRACE_SEC = float(os.environ.get("LEADER_GRACE_SEC", str(DOWN_AFTER_MISSES*HEARTBEAT_INTERVAL + 2.0)))

# Lease for Telegram polling is enforced via distributed RPC coordination.
BOT_LEASE_TTL = int(os.environ.get("BOT_LEASE_TTL", "10"))  # секунд


# --- Persistent storage & local state ---------------------------------------------

def now_s() -> int:
    return int(time.time())


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data: Any):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# --- Metrics & telemetry helpers --------------------------------------------------

def _run_speedtest_blocking() -> Dict[str, Any]:
    try:
        import speedtest
    except Exception:
        return {"ok": False, "error": "speedtest-cli not installed (pip install speedtest-cli)"}
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        down = st.download() / 1e6  # Mbps
        up = st.upload() / 1e6      # Mbps
        ping = st.results.ping
        return {
            "ok": True,
            "down_mbps": round(down, 2),
            "up_mbps": round(up, 2),
            "ping_ms": round(ping, 1),
        }
    except Exception as e:
        return {"ok": False, "error": f"{e}"}


async def run_local_speedtest() -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _run_speedtest_blocking)
    except Exception as e:
        return {"ok": False, "error": f"executor_error:{e}"}

def _filter_last_hours(samples: deque, hours: int) -> list[tuple[int, float]]:
    cutoff = now_s() - hours * 3600
    return [(ts, v) for ts, v in samples if ts >= cutoff]


async def telemetry_loop():
    """Collects periodic CPU and optional speedtest metrics in the background."""

    # Fast initial snapshot so the UI has data immediately after startup.
    CPU_SAMPLES.append((now_s(), psutil.cpu_percent(interval=0.2)))
    if ENABLE_BG_SPEEDTEST:
        # Do not block startup; placeholders will be replaced by the first real run.
        NET_DOWN_SAMPLES.append((now_s(), 0.0))
        NET_UP_SAMPLES.append((now_s(), 0.0))

    while True:
        ts = now_s()
        CPU_SAMPLES.append((ts, psutil.cpu_percent(interval=0.2)))

        if ENABLE_BG_SPEEDTEST and not SPEEDTEST_LOCK.locked():
            async with SPEEDTEST_LOCK:
                res = await run_local_speedtest()
            if res.get("ok"):
                ts_now = now_s()
                NET_DOWN_SAMPLES.append((ts_now, float(res["down_mbps"])))
                NET_UP_SAMPLES.append((ts_now, float(res["up_mbps"])))
                logger.debug(
                    "[telemetry] background speedtest ok",
                    extra={
                        "down": res.get("down_mbps"),
                        "up": res.get("up_mbps"),
                        "ping": res.get("ping_ms"),
                    },
                )
            else:
                logger.warning(
                    "[telemetry] background speedtest failed",
                    extra={"error": res.get("error")},
                )
        await asyncio.sleep(SAMPLE_EVERY_SEC)


def summarize_series_points(
    series: List[Tuple[int, Any]],
    limit: int = METRICS_SUMMARY_POINTS,
) -> Tuple[int, float, float]:
    if not series:
        return 0, 0.0, 0.0
    ordered = sorted(series, key=lambda x: x[0])
    trimmed = ordered[-max(1, limit):]
    values = [float(v) for _, v in trimmed]
    avg = sum(values) / len(values)
    max_v = max(values)
    return len(trimmed), avg, max_v


def summarize_net_points(
    down: List[Tuple[int, Any]],
    up: List[Tuple[int, Any]],
    limit: int = METRICS_SUMMARY_POINTS,
) -> Tuple[int, float, float]:
    down_ordered = sorted(down, key=lambda x: x[0]) if down else []
    up_ordered = sorted(up, key=lambda x: x[0]) if up else []
    down_trim = down_ordered[-max(1, limit):] if down_ordered else []
    up_trim = up_ordered[-max(1, limit):] if up_ordered else []
    avg_down = sum(float(v) for _, v in down_trim) / len(down_trim) if down_trim else 0.0
    avg_up = sum(float(v) for _, v in up_trim) / len(up_trim) if up_trim else 0.0
    count = max(len(down_trim), len(up_trim))
    return count, avg_down, avg_up


def sample_window_label(count: int, singular: str, plural: str) -> str:
    if count == 1:
        return f"последнее {singular}"
    return f"последние {count} {plural}"


def friendly_error_message(err: Optional[str]) -> str:
    if not err:
        return "неизвестная ошибка"
    text = str(err)
    if text.startswith("rpc_error:"):
        text = text.split("rpc_error:", 1)[1]
    if "timeout" in text.lower():
        return "таймаут запроса"
    return text

# Local persistent cache with network topology & bot lease info
state = load_json(STATE_FILE, {
    "network_id": NETWORK_ID or "",
    "owner_username": OWNER_USERNAME or "",
    "network_secret": NETWORK_SECRET or "",
    "peers": [],  # [{name, addr, node_id, status, last_seen}]
    "bot_lease": {"owner": "", "until": 0}
})

# Outstanding invite tokens (for onboarding new peers)
invites = load_json(INVITES_FILE, {
    "tokens": []  # [{token, exp_ts}]
})

# Уникальный id узла (стабилен между перезапусками)
NODE_ID_FILE = os.path.join(STATE_DIR, "node_id")
if os.path.exists(NODE_ID_FILE):
    with open(NODE_ID_FILE, "r") as f:
        NODE_ID = f.read().strip()
else:
    NODE_ID = hashlib.sha256(f"{SERVER_NAME}-{uuid.uuid4().hex}".encode()).hexdigest()
    with open(NODE_ID_FILE, "w") as f:
        f.write(NODE_ID)

# In-memory peer table keeps the freshest view for fast lookups
peers: Dict[str, Dict[str, Any]] = {}
self_peer = {"name": SERVER_NAME, "addr": PUBLIC_ADDR, "node_id": NODE_ID, "status": "alive", "last_seen": now_s()}

# Telegram globals
BOT: Optional["Bot"] = None
DP: Optional["Dispatcher"] = None
BOT_TASK: Optional[asyncio.Task] = None
BOT_RUN_GEN = 0   # глобальный счётчик поколений
BOT_LOCK = asyncio.Lock()
BOT_RUNNING_OWNER: Optional[str] = None
BOT_LAST_BROADCAST_UNTIL = 0
BOT_LAST_BROADCAST_OWNER: Optional[str] = None

# --- RPC payload signing ---------------------------------------------------------
def canonical_json(d: Dict[str, Any]) -> str:
    return json.dumps(d, separators=(",", ":"), sort_keys=True)

def make_sig(payload: Dict[str, Any], secret: str) -> str:
    msg = canonical_json(payload).encode()
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()

def verify_sig(payload: Dict[str, Any], secret: str) -> bool:
    sig = payload.get("sig", "")
    if "sig" in payload:  # проверяем на копии без sig
        payload = dict(payload)
        payload.pop("sig", None)
    if "ts" not in payload: return False
    if abs(now_s() - int(payload["ts"])) > CLOCK_SKEW:  # анти-replay по времени
        return False
    calc = make_sig(payload, secret)
    return hmac.compare_digest(calc, sig)


# --- Bot lease helpers -----------------------------------------------------------

def set_bot_lease(owner: str, until: int):
    state["bot_lease"] = {"owner": owner, "until": until}
    save_json(STATE_FILE, state)

def get_bot_lease():
    bl = state.get("bot_lease", {}) or {}
    return bl.get("owner",""), int(bl.get("until",0))

# --- Peer table helpers ----------------------------------------------------------
def set_state(k: str, v: Any):
    state[k] = v
    save_json(STATE_FILE, state)

def upsert_peer(p: Dict[str, Any]):
    if not p.get("node_id"): return
    cur = peers.get(p["node_id"], {})
    cur.update(p)
    peers[p["node_id"]] = cur
    # синхронизируем в state.peers
    found = False
    for item in state["peers"]:
        if item.get("node_id") == p["node_id"]:
            item.update(cur)
            found = True
            break
    if not found:
        state["peers"].append(cur.copy())
    save_json(STATE_FILE, state)

def is_peer_online(last_seen: Optional[int], *, now: Optional[int] = None) -> bool:
    if last_seen is None:
        return False
    if now is None:
        now = now_s()
    try:
        last = int(last_seen)
    except (TypeError, ValueError):
        return False
    if last <= 0:
        return False
    return (now - last) <= HEARTBEAT_TIMEOUT

def get_alive_peers() -> List[Dict[str, Any]]:
    alive = []
    now = now_s()
    for p in [*peers.values(), self_peer]:
        status = peer_status(p, now=now)
        p["status"] = status
        if status == "alive":
            alive.append(p)
    return alive

def compute_leader_key(p: Dict[str, Any]) -> Tuple[int, str]:
    return (int(p.get("priority", 0) or 0), p.get("node_id",""))

def current_leader() -> Dict[str, Any]:
    candidates = [p for p in peers_with_status() if p.get("status") == "alive"]
    # включаем себя, если вдруг не попали
    if not any(p.get("node_id") == NODE_ID for p in candidates):
        me = dict(self_peer); me["status"] = "alive"
        candidates.append(me)
    return min(candidates, key=compute_leader_key)

def i_am_leader() -> bool:
    L = current_leader()
    return L.get("node_id") == NODE_ID

async def safe_edit(msg, text: str, *, reply_markup=None, parse_mode=None) -> bool:
    """Edit a message in place, tolerating common Telegram errors."""
    try:
        await msg.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as e:
        err = str(e)
        if "message is not modified" in err:
            bot_logger.debug("safe_edit: message already up-to-date", extra={"chat_id": msg.chat.id, "message_id": msg.message_id})
            if reply_markup is not None:
                try:
                    await msg.edit_reply_markup(reply_markup)
                except TelegramBadRequest as e2:
                    bot_logger.debug("safe_edit: reply_markup already up-to-date", extra={"chat_id": msg.chat.id, "message_id": msg.message_id, "error": str(e2)})
            return True
        if any(key in err.lower() for key in ["message to edit not found", "message can't be edited", "message_id_invalid"]):
            bot_logger.warning("safe_edit: target message unavailable", extra={"chat_id": msg.chat.id, "message_id": msg.message_id, "error": err})
            return False
        bot_logger.warning("safe_edit: unexpected Telegram error", extra={"chat_id": msg.chat.id, "message_id": msg.message_id, "error": err})
        raise
    except Exception as e:
        bot_logger.exception("safe_edit: unexpected exception", extra={"chat_id": getattr(msg.chat, 'id', None), "message_id": getattr(msg, 'message_id', None)})
        raise

async def safe_edit_message(bot, chat_id: int, message_id: int, text: str, *, reply_markup=None, parse_mode=None) -> bool:
    """Same as safe_edit but operates on chat/message ids."""
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as e:
        err = str(e)
        if "message is not modified" in err:
            bot_logger.debug("safe_edit_message: message already up-to-date", extra={"chat_id": chat_id, "message_id": message_id})
            if reply_markup is not None:
                try:
                    await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)
                except TelegramBadRequest as e2:
                    bot_logger.debug("safe_edit_message: reply_markup already up-to-date", extra={"chat_id": chat_id, "message_id": message_id, "error": str(e2)})
            return True
        if any(key in err.lower() for key in ["message to edit not found", "message can't be edited", "message_id_invalid"]):
            bot_logger.warning("safe_edit_message: target message unavailable", extra={"chat_id": chat_id, "message_id": message_id, "error": err})
            return False
        bot_logger.warning("safe_edit_message: unexpected Telegram error", extra={"chat_id": chat_id, "message_id": message_id, "error": err})
        raise
    except Exception:
        bot_logger.exception("safe_edit_message: unexpected exception", extra={"chat_id": chat_id, "message_id": message_id})
        raise

# --- Metrics snapshot helpers ----------------------------------------------------
def collect_stats() -> Dict[str, Any]:
    cpu = psutil.cpu_percent(interval=0.2, percpu=True)
    vm = psutil.virtual_memory()
    du = psutil.disk_usage("/")
    return {
        "server_name": SERVER_NAME,
        "uptime_s": int(time.time() - psutil.boot_time()),
        "cpu_per_core_pct": cpu,
        "ram": {"total_mb": vm.total // (1024*1024), "used_mb": (vm.total - vm.available) // (1024*1024), "pct": round(vm.percent,2)},
        "disk_root": {"total_gb": round(du.total / (1024**3),1), "used_gb": round(du.used / (1024**3),1), "pct": round(du.percent,2)},
    }

# --- HTTP server & RPC endpoints -------------------------------------------------
routes = web.RouteTableDef()
http_client: Optional[ClientSession] = None
HTTP_CLIENT_LOCK = asyncio.Lock()


async def ensure_http_client() -> ClientSession:
    """Return a shared aiohttp session, creating it lazily when needed."""
    global http_client
    async with HTTP_CLIENT_LOCK:
        if http_client is None or http_client.closed:
            http_client = ClientSession()
        return http_client

@routes.get("/health")
async def health(req):
    return web.json_response({"ok": True, "name": SERVER_NAME, "node_id": NODE_ID, "ts": now_s()})

@routes.get("/peers")
async def get_peers_http(req):
    return web.json_response({"peers": peers_with_status()})

@routes.get("/join_handshake")
async def join_handshake(req):
    """
    Read-only рукопожатие: отдаём базовую сетевую инфу,
    чтобы новый узел мог сверить сетевые настройки до фактического join.
    """
    qs = req.rel_url.query
    net = qs.get("net", "")
    # опционально сверяем network_id, если задан
    if net and state.get("network_id") and net != state["network_id"]:
        return web.json_response({"ok": False, "reason": "wrong network"}, status=403)

    return web.json_response({
        "ok": True,
        "network_id": state.get("network_id"),
        "owner_username": state.get("owner_username"),
        "seed_peers": [p.get("addr") for p in state.get("peers", []) if p.get("addr")] or ([PUBLIC_ADDR] if PUBLIC_ADDR else []),
    })


@routes.post("/join")
async def join(req):
    """
    JOIN: {name, token, network_id, public_addr}
    Ответ: {ok, reason?, network_id, owner_username, network_secret, peers[]}
    """
    data = await req.json()
    name = data.get("name","")
    token = data.get("token","")
    net = data.get("network_id","")
    pub_addr = data.get("public_addr","")

    if not name or not token or not net or not pub_addr:
        return web.json_response({"ok": False, "reason": "bad request"}, status=400)

    if net != state.get("network_id"):
        return web.json_response({"ok": False, "reason": "wrong network"}, status=403)

    # проверка токена
    nowt = now_s()
    valid = False
    tokens = invites.get("tokens", [])
    keep = []
    for t in tokens:
        if t["token"] == token and t["exp_ts"] >= nowt:
            valid = True
        else:
            keep.append(t)
    invites["tokens"] = keep
    save_json(INVITES_FILE, invites)

    if not valid:
        return web.json_response({"ok": False, "reason": "invalid/expired token"}, status=403)

    # Регистрируем нового пира
    new_peer = {
        "name": name,
        "addr": pub_addr,
        "node_id": "",
        "status": "alive",
        "last_seen": now_s()
    }

    peers_list = state.get("peers", [])
    peers_list.append({"name": name, "addr": pub_addr, "node_id": "", "status": "alive", "last_seen": now_s()})
    set_state("peers", peers_list)

    upsert_peer(new_peer)

    # Обновляем состояние в памяти и на диске
    print(f"[join] accepted new peer {name} ({pub_addr})")
    save_json(STATE_FILE, state)

    # Рассылаем остальным пинг, чтобы они увидели нового участника
    asyncio.create_task(propagate_new_peer(new_peer))

    set_state("join_url", "")

    return web.json_response({
        "ok": True,
        "network_id": state.get("network_id"),
        "owner_username": state.get("owner_username"),
        "network_secret": state.get("network_secret"),
        "peers": state.get("peers", [])
    })

@routes.post("/announce")
async def announce(req):
    try:
        data = await req.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad json"}, status=400)

    name = data.get("name","")
    addr = data.get("addr","")
    node_id = data.get("node_id","")
    if not name or not addr:
        return web.json_response({"ok": False, "error": "bad request"}, status=400)

    upsert_peer({
        "name": name, "addr": addr, "node_id": node_id or "",
        "status": "alive", "last_seen": now_s()
    })
    return web.json_response({"ok": True})


@routes.post("/rpc")
async def rpc(req):
    """
    JSON RPC with HMAC:
    { "method": "...", "params": {...}, "ts": 123, "sig": "hex" }
    """
    if not state.get("network_secret"):
        return web.json_response({"ok": False, "error": "no network secret"}, status=403)
    payload = await req.json()
    if not verify_sig(payload, state["network_secret"]):
        return web.json_response({"ok": False, "error": "bad signature"}, status=403)
    method = payload.get("method","")
    params = payload.get("params", {}) or {}
    if method == "GetPeers":
        return web.json_response({"ok": True, "peers": peers_with_status()})
    elif method == "GetStats":
        target = params.get("target")
        if target and target not in (SERVER_NAME, NODE_ID):
            # проксируем дальше?
            return web.json_response({"ok": False, "error": "target mismatch"}, status=400)
        return web.json_response({"ok": True, "stats": collect_stats()})
    elif method == "Reboot":
        target = params.get("target")
        if target and target not in (SERVER_NAME, NODE_ID):
            return web.json_response({"ok": False, "error": "target mismatch"}, status=400)
        # Требует соответствующих прав (CAP_SYS_BOOT / root)
        asyncio.create_task(async_reboot())
        return web.json_response({"ok": True, "message": "rebooting"})

    elif method == "GetLease":
        owner, until = get_bot_lease()
        return web.json_response({"ok": True, "owner": owner, "until": until, "now": now_s()})

    elif method == "TryAcquireLease":
        # params: {"candidate": NODE_ID, "ttl": seconds}
        cand = params.get("candidate", "")
        ttl = int(params.get("ttl", BOT_LEASE_TTL))
        nowt = now_s()
        owner, until = get_bot_lease()
        # если лиз ещё активен у другого — отказываем
        if owner and owner != cand and until > nowt:
            return web.json_response({"ok": False, "owner": owner, "until": until})
        # иначе выдаём лиз кандидату
        set_bot_lease(cand, nowt + ttl)
        return web.json_response({"ok": True, "owner": cand, "until": nowt + ttl})

    elif method == "ReleaseLease":
        cand = params.get("candidate", "")
        owner, until = get_bot_lease()
        # освобождать может владелец или истёкший
        if owner == cand or until <= now_s():
            set_bot_lease("", 0)
            return web.json_response({"ok": True})
        return web.json_response({"ok": False, "owner": owner, "until": until})

    elif method == "Lease.Get":
        lease = state.get("bot_lease", {"owner": "", "until": 0})
        return web.json_response({"ok": True, "owner": lease.get("owner", ""), "until": lease.get("until", 0)})

    elif method == "Lease.Acquire":
        want = params.get("owner", "")
        ttl = int(params.get("ttl", BOT_LEASE_TTL))
        nowt = now_s()
        lease = state.get("bot_lease", {"owner": "", "until": 0})
        # если истёк или свободен — отдаём
        if not lease.get("owner") or lease.get("until", 0) <= nowt or lease.get("owner") == want:
            lease = {"owner": want, "until": nowt + ttl}
            state["bot_lease"] = lease
            save_json(STATE_FILE, state)
            return web.json_response({"ok": True, "owner": lease["owner"], "until": lease["until"]})
        else:
            return web.json_response({"ok": False, "owner": lease.get("owner", ""), "until": lease.get("until", 0)})

    elif method == "Lease.Release":
        who = params.get("owner", "")
        lease = state.get("bot_lease", {"owner": "", "until": 0})
        if lease.get("owner") == who:
            state["bot_lease"] = {"owner": "", "until": 0}
            save_json(STATE_FILE, state)
            return web.json_response({"ok": True})
        return web.json_response({"ok": True})  # идемпотентно

    elif method == "Bot.Takeover":
        new_owner = params.get("owner", "")
        until = int(params.get("until", 0) or 0)
        nowt = now_s()
        display = new_owner[:8] if new_owner else "<none>"
        print(f"[rpc] takeover request: owner={display} until={until}")
        # если новый владелец не мы — обязательно гасим локальный бот
        should_stop = new_owner != NODE_ID or until <= nowt
        stopped = False
        if should_stop and bot_task_running():
            print(f"[rpc] takeover: stopping bot for new owner {display}")
            await stop_bot()
            stopped = True
        set_bot_lease(new_owner, until)
        if new_owner != NODE_ID:
            # запоминаем в глобальном состоянии, что лидер сменился
            global BOT_RUNNING_OWNER
            BOT_RUNNING_OWNER = new_owner if new_owner else None
        running = bot_task_running()
        return web.json_response({
            "ok": True,
            "stopped": stopped,
            "running": running,
            "owner": new_owner,
            "until": until
        })

    elif method == "GetTS":
        kind = (params.get("kind") or "").lower()
        hours = int(params.get("hours", 6))
        if kind == "cpu":
            data = _filter_last_hours(CPU_SAMPLES, hours)
            return web.json_response({"ok": True, "kind": "cpu", "series": data})
        elif kind == "net":
            d = _filter_last_hours(NET_DOWN_SAMPLES, hours)
            u = _filter_last_hours(NET_UP_SAMPLES, hours)
            return web.json_response({"ok": True, "kind": "net", "down": d, "up": u})
        else:
            return web.json_response({"ok": False, "error": "unknown timeseries kind"}, status=400)

    elif method == "RunSpeedtest":
        # принудительный спидтест «сейчас»
        if SPEEDTEST_LOCK.locked():
            rpc_logger.warning(
                "RunSpeedtest rejected: already running",
                extra={"server": SERVER_NAME},
            )
            return web.json_response({"ok": False, "error": "another speedtest running"})
        started = time.time()
        async with SPEEDTEST_LOCK:
            rpc_logger.info("RunSpeedtest local start", extra={"server": SERVER_NAME})
            try:
                res = await run_local_speedtest()
            except Exception as e:
                rpc_logger.exception("RunSpeedtest local exception", extra={"server": SERVER_NAME})
                res = {"ok": False, "error": f"internal_error:{e}"}
        duration_ms = int((time.time() - started) * 1000)
        if res.get("ok"):
            ts = now_s()
            NET_DOWN_SAMPLES.append((ts, float(res["down_mbps"])))
            NET_UP_SAMPLES.append((ts, float(res["up_mbps"])))
            rpc_logger.info(
                "RunSpeedtest local success",
                extra={
                    "server": SERVER_NAME,
                    "duration_ms": duration_ms,
                    "down": res.get("down_mbps"),
                    "up": res.get("up_mbps"),
                    "ping": res.get("ping_ms"),
                },
            )
        else:
            rpc_logger.warning(
                "RunSpeedtest local failed",
                extra={"server": SERVER_NAME, "duration_ms": duration_ms, "error": res.get("error")},
            )
        return web.json_response(res)

    else:
        return web.json_response({"ok": False, "error": "unknown method"}, status=400)


# --- RPC client helpers ----------------------------------------------------------

async def call_rpc(addr: str, method: str, params: Dict[str, Any], *, timeout: Optional[float] = None) -> Dict[str, Any]:
    if not state.get("network_secret"):
        return {"ok": False, "error": "no_network_secret"}
    payload = {"method": method, "params": params, "ts": now_s()}
    payload["sig"] = make_sig(payload, state["network_secret"])
    url = f"http://{addr}/rpc"
    client = await ensure_http_client()
    try:
        total = timeout if timeout is not None else RPC_TIMEOUT
        async with client.post(url, json=payload, timeout=ClientTimeout(total=total)) as r:
            return await r.json()
    except Exception as e:
        return {"ok": False, "error": f"rpc_error:{e}"}


async def get_lease(addr: str):
    return await call_rpc(addr, "GetLease", {})

async def try_acquire_lease(addr: str, candidate: str, ttl: int):
    return await call_rpc(addr, "TryAcquireLease", {"candidate": candidate, "ttl": ttl})

async def release_lease(addr: str, candidate: str):
    return await call_rpc(addr, "ReleaseLease", {"candidate": candidate})

async def lease_get_from(coord_addr: str) -> Dict[str, Any]:
    return await call_rpc(coord_addr, "Lease.Get", {})

async def lease_acquire_from(coord_addr: str, owner: str, ttl: int) -> Dict[str, Any]:
    return await call_rpc(coord_addr, "Lease.Acquire", {"owner": owner, "ttl": ttl})

async def lease_release_from(coord_addr: str, owner: str) -> Dict[str, Any]:
    return await call_rpc(coord_addr, "Lease.Release", {"owner": owner})

async def bot_takeover(addr: str, owner: str, until: int) -> Dict[str, Any]:
    return await call_rpc(addr, "Bot.Takeover", {"owner": owner, "until": until})


# --- Lease coordination & leader election ----------------------------------------

async def propagate_bot_lease(owner: str, until: int, *, force_takeover: bool = False):
    """Update local lease view and notify peers only when ownership changes.

    Silent renewals keep updating ``state['bot_lease']`` but avoid broadcasting
    Bot.Takeover RPCs so followers do not spam their logs. When ``owner`` changes
    (leadership hand-over) or ``force_takeover`` is requested we fan out the
    takeover notification exactly once.
    """

    global BOT_LAST_BROADCAST_UNTIL, BOT_LAST_BROADCAST_OWNER

    set_bot_lease(owner, until)
    BOT_LAST_BROADCAST_UNTIL = until if owner else 0

    normalized_owner = owner or None
    takeover_needed = force_takeover or (normalized_owner != BOT_LAST_BROADCAST_OWNER)
    if not takeover_needed:
        return

    BOT_LAST_BROADCAST_OWNER = normalized_owner

    peers = [p for p in get_alive_peers() if p.get("node_id") != NODE_ID and p.get("addr")]
    if not peers:
        return

    async def notify(p):
        addr = p.get("addr")
        name = p.get("name") or addr
        try:
            res = await bot_takeover(addr, owner, until)
            if res.get("ok"):
                stopped = res.get("stopped")
                running = res.get("running")
                print(f"[lease] takeover -> {name}: stopped={stopped} running={running}")
            else:
                print(f"[lease] takeover rejected by {name}: {res}")
        except Exception as e:
            print(f"[lease] takeover notify failed for {name}: {e}")

    await asyncio.gather(*(notify(p) for p in peers), return_exceptions=True)

async def rpc_get_ts(addr: str, kind: str, hours: int = 6) -> Dict[str, Any]:
    return await call_rpc(addr, "GetTS", {"kind": kind, "hours": hours})

async def rpc_speedtest(addr: str) -> Dict[str, Any]:
    return await call_rpc(addr, "RunSpeedtest", {}, timeout=SPEEDTEST_RPC_TIMEOUT)

async def propagate_new_peer(new_peer):
    """Рассылаем информацию о новом пире всем живым узлам"""
    await asyncio.sleep(0.3)
    for p in get_alive_peers():
        if p["addr"] == new_peer["addr"]:
            continue
        try:
            await call_rpc(
                p["addr"],
                "GetPeers",
                {"note": f"new peer {new_peer['name']}"}
            )
        except Exception as e:
            print(f"[propagate] failed to contact {p['addr']}: {e}")


async def async_reboot():
    await asyncio.sleep(0.2)
    cmd = "/usr/bin/nsenter -t 1 -m -u -i -n -p /sbin/reboot"
    os.system("sync")
    os.system(cmd)

# --- Heartbeat & peer discovery --------------------------------------------------
async def heartbeat_loop():
    await asyncio.sleep(0.1)
    # первичное заполнение peers из state (если было)
    for p in state.get("peers", []):
        upsert_peer(p)

    # также добавим seed адреса (без node_id)
    for addr in SEED_PEERS:
        upsert_peer({"name": addr, "addr": addr, "node_id": "", "status": "unknown", "last_seen": 0})

    while True:
        # 1) опрос известных адресов /health
        for node_id, p in list(peers.items()):
            addr = p.get("addr")
            if not addr:
                continue
            try:
                async with http_client.get(f"http://{addr}/health", timeout=ClientTimeout(total=RPC_TIMEOUT)) as r:
                    if r.status == 200:
                        data = await r.json()
                        nid = data.get("node_id", "")
                        nm = data.get("name", p.get("name"))
                        info = {"name": nm, "addr": addr, "node_id": nid, "status": "alive", "last_seen": now_s()}
                        upsert_peer(info)
                    else:
                        # ошибка — пусть last_seen устареет
                        pass
            except Exception:
                # нет ответа — пусть last_seen устареет
                pass

        # 2) обновим локальное представление себя (для /peers)
        self_peer.update({"addr": PUBLIC_ADDR, "last_seen": now_s(), "status": "alive"})

        # объявляем себя известным адресам (лидер после рестарта нас увидит)
        targets = {p.get("addr") for p in state.get("peers", []) if p.get("addr")}
        myaddr = PUBLIC_ADDR
        if myaddr in targets:
            targets.discard(myaddr)
        for addr in list(targets):
            try:
                await http_client.post(
                    f"http://{addr}/announce",
                    json={"name": SERVER_NAME, "addr": PUBLIC_ADDR, "node_id": NODE_ID},
                    timeout=ClientTimeout(total=RPC_TIMEOUT)
                )
            except Exception:
                pass

        await asyncio.sleep(HEARTBEAT_INTERVAL)

def peer_status(p: Optional[Dict[str, Any]], *, now: Optional[int] = None) -> str:
    if not p:
        return "offline"
    last_seen = p.get("last_seen")
    if now is None:
        now = now_s()
    return "alive" if is_peer_online(last_seen, now=now) else "offline"


STATUS_EMOJI = {
    "alive": "🟢",
    "offline": "🔴",
}


def peer_status_icon(status: str) -> str:
    return STATUS_EMOJI.get(status, "⚪️")


def peers_with_status() -> List[Dict[str, Any]]:
    # объединяем известных пиров и себя; статусы считаем на лету
    merged = {p.get("node_id", ""): dict(p) for p in state.get("peers", [])}
    merged[NODE_ID] = dict(self_peer)
    now = now_s()
    out = []
    for nid, p in merged.items():
        q = dict(p)
        q.setdefault("node_id", nid)
        name = q.get("name") or q.get("addr") or (q.get("node_id") or "")[:8] or "?"
        q["name"] = name
        q["status"] = peer_status(q, now=now)
        q["status_emoji"] = peer_status_icon(q["status"])
        out.append(q)
    return out

# --- Network bootstrap (JOIN flow) -----------------------------------------------
def parse_join_url(u: str) -> Tuple[str, Dict[str, str]]:
    # join://host:port?net=...&token=...&ttl=...
    assert u.startswith("join://")
    rest = u[len("join://"):]
    host, _, q = rest.partition("?")
    qs = {}
    for part in q.split("&"):
        if not part: continue
        k, _, v = part.partition("=")
        qs[k] = v
    return host, qs

async def do_join_if_needed():
    print("[join] checking join conditions...")

    # Если уже есть непустой state -> не делаем join
    if os.path.exists(STATE_FILE):
        try:
            st = load_json(STATE_FILE, {})
            if st.get("network_id"):
                return
        except Exception:
            pass

    if not JOIN_URL:
        # режим init — state должен быть уже создан install.sh init-ом
        return

    seed, qs = parse_join_url(JOIN_URL)
    net = qs.get("net", "")
    token = qs.get("token", "")
    if not net or not token:
        print("JOIN_URL missing net/token", file=sys.stderr)
        return

    payload = {
        "name": SERVER_NAME,
        "token": token,
        "network_id": net,
        "public_addr": PUBLIC_ADDR,
    }

    try:
        async with http_client.post(f"http://{seed}/join", json=payload, timeout=ClientTimeout(total=8)) as r:
            print(f"[join] sending join to {seed}…")
            data = await r.json()
    except Exception as e:
        print("join error:", e, file=sys.stderr)
        return

    if not data.get("ok"):
        print("join refused:", data, file=sys.stderr)
        return

    # записываем state
    set_state("network_id", data["network_id"])
    set_state("owner_username", data["owner_username"])
    set_state("network_secret", data["network_secret"])
    set_state("peers", data.get("peers", []))

    # добавим seed в peers, если его нет
    present = any(p.get("addr") == seed for p in state["peers"])
    if not present:
        upsert_peer({"name": seed, "addr": seed, "node_id": "", "status": "unknown", "last_seen": 0})

    print(f"[join] Joined network {data['network_id']} via {seed}")


# --- Telegram bot orchestration --------------------------------------------------

def normalized_owner() -> str:
    u = state.get("owner_username","").strip()
    return u[1:] if u.startswith("@") else u

def bot_task_running() -> bool:
    return BOT_TASK is not None and not BOT_TASK.done()

async def start_bot():
    """Start the aiogram polling loop once we are the lease holder."""
    global BOT, DP, BOT_TASK, BOT_RUN_GEN, BOT_RUNNING_OWNER

    async with BOT_LOCK:
        # если уже запущен — не плодим дубликаты
        if bot_task_running():
            print("[bot] already running; skip")
            return

        BOT = DP = None  # ensure reset before creation

        from aiogram import Bot, Dispatcher, types, F
        from aiogram.filters import Command
        from aiogram.utils.keyboard import InlineKeyboardBuilder

        # --- Inline UI state & helpers (single owner) ---
        UI = {}  # chat_id -> {"msg_id": int, "page": int, "selected": Optional[str]}

        PAGE_SIZE = 6

        BOT = Bot(BOT_TOKEN)
        DP = Dispatcher()

        # зафиксируем «поколение» запуска для этого инстанса
        BOT_RUN_GEN += 1
        my_gen = BOT_RUN_GEN
        BOT_RUNNING_OWNER = NODE_ID

    owner = normalized_owner()

    def describe_user(obj) -> str:
        user = getattr(obj, "from_user", None)
        if not user:
            return "unknown"
        if user.username:
            return f"@{user.username}"
        return f"id:{user.id}"

    def event_chat_id(obj) -> Optional[int]:
        if isinstance(obj, types.Message):
            return obj.chat.id
        if isinstance(obj, types.CallbackQuery) and obj.message:
            return obj.message.chat.id
        return None

    def only_owner(handler):
        @wraps(handler)
        async def wrapper(event, *a, **k):
            user = getattr(event, "from_user", None)
            username = (user.username or "").lower() if user and user.username else ""
            if owner and username != owner.lower():
                bot_logger.debug(
                    "ignore interaction from non-owner",
                    extra={"chat_id": event_chat_id(event), "user": describe_user(event)},
                )
                if isinstance(event, types.CallbackQuery):
                    try:
                        await event.answer("Доступ запрещён", show_alert=True)
                    except Exception:
                        pass
                return
            return await handler(event, *a, **k)
        return wrapper

    def bot_action(action_name: str):
        def decorator(func):
            @wraps(func)
            async def wrapper(event, *a, **k):
                chat_id = event_chat_id(event)
                data = getattr(event, "data", None)
                bot_logger.info(
                    f"[bot] action {action_name}",
                    extra={"chat_id": chat_id, "data": data, "user": describe_user(event)},
                )
                try:
                    return await func(event, *a, **k)
                except Exception as e:
                    bot_logger.exception(
                        f"[bot] action {action_name} failed",
                        extra={"chat_id": chat_id, "data": data, "user": describe_user(event)},
                    )
                    if isinstance(event, types.CallbackQuery):
                        try:
                            await event.answer(f"Ошибка: {e}", show_alert=True)
                        except Exception:
                            pass
                    return
            return wrapper
        return decorator

    def ensure_ui(chat_id: int) -> dict:
        st = UI.get(chat_id)
        if not st:
            st = {"msg_id": 0, "page": 0, "selected": None}
            UI[chat_id] = st
        return st

    def resolve_target(name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not name:
            return None, None
        if name == SERVER_NAME:
            peer = dict(self_peer)
            peer["status"] = "alive"
            peer["addr"] = LISTEN_ADDR
            return peer, LISTEN_ADDR
        for peer in peers_with_status():
            if peer.get("name") == name:
                addr = peer.get("addr")
                if peer.get("node_id") == NODE_ID and not addr:
                    addr = LISTEN_ADDR
                return peer, addr
        return None, None

    def format_server_title(name: str, status: str, is_host: bool) -> str:
        if status == "alive":
            suffix = " — *Хост*" if is_host else ""
            return f"Сервер *{name}*{suffix}"
        return f"Сервер *{name}* — Offline"

    def describe_server(name: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str], str, bool]:
        peer, addr = resolve_target(name)
        now = now_s()
        status = peer_status(peer, now=now)
        is_host = False
        try:
            leader = current_leader()
            if peer and peer.get("node_id") and leader.get("node_id") == peer.get("node_id"):
                is_host = True
        except Exception as e:
            bot_logger.debug(
                "describe_server: leader lookup failed",
                extra={"server": name, "error": str(e)},
            )
        title = format_server_title(name, status, is_host)
        if peer is not None:
            peer = dict(peer)
            peer["status"] = status
        return title, peer, addr, status, is_host

    # Inline keyboard builders --------------------------------------------------

    def build_nodes_page(page: int) -> types.InlineKeyboardMarkup:
        peers = sorted(peers_with_status(), key=lambda p: p.get("name", ""))
        total = len(peers)
        start = page * PAGE_SIZE
        chunk = peers[start:start + PAGE_SIZE]
        kb = InlineKeyboardBuilder()
        for p in chunk:
            name = p.get("name")
            status = (p.get("status") or "").lower()
            icon = p.get("status_emoji") or peer_status_icon(status)
            kb.button(text=f"{icon} {name}", callback_data=f"server:{name}")
        if chunk:
            kb.adjust(2)
        else:
            kb.adjust(1)
        pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if pages > 1:
            nav = InlineKeyboardBuilder()
            prev_p = (page - 1) % pages
            next_p = (page + 1) % pages
            nav.button(text="⟨", callback_data=f"page:{prev_p}")
            nav.button(text=f"{page + 1}/{pages}", callback_data="noop")
            nav.button(text="⟩", callback_data=f"page:{next_p}")
            kb.row(*nav.buttons)
        return kb.as_markup()

    def build_server_menu(name: str) -> types.InlineKeyboardMarkup:
        _, peer, _, status, _ = describe_server(name)
        alive = status == "alive"
        kb = InlineKeyboardBuilder()
        if alive:
            kb.button(text="📊 Stats", callback_data=f"action:stats:{name}")
            kb.button(text="🌐 Network", callback_data=f"action:net:{name}")
            kb.button(text="📈 Metrics", callback_data=f"action:metrics:{name}")
            kb.button(text="🔄 Reboot", callback_data=f"action:reboot:{name}")
            kb.adjust(2, 2)
        else:
            kb.button(text="Сервер оффлайн", callback_data="noop")
            kb.adjust(1)
        kb.button(text="← Назад к списку", callback_data="back:nodes")
        return kb.as_markup()

    def build_reboot_confirm(name: str) -> types.InlineKeyboardMarkup:
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Да, перезагрузить", callback_data=f"action:reboot_yes:{name}")
        kb.button(text="↩️ Отмена", callback_data=f"action:reboot_back:{name}")
        kb.adjust(2)
        return kb.as_markup()

    async def ensure_ui_message(m: types.Message) -> tuple[int, dict]:
        chat_id = m.chat.id
        st = ensure_ui(chat_id)
        if st["msg_id"]:
            bot_logger.debug(
                "ensure_ui_message: reuse",
                extra={"chat_id": chat_id, "message_id": st["msg_id"]},
            )
            return st["msg_id"], st
        sent = await m.answer("Выберите сервер:", reply_markup=build_nodes_page(st["page"]))
        st["msg_id"] = sent.message_id
        UI[chat_id] = st
        bot_logger.info(
            "ensure_ui_message: created",
            extra={"chat_id": chat_id, "message_id": st["msg_id"]},
        )
        return st["msg_id"], st

    async def edit_ui(bot: "Bot", chat_id: int, st: dict, text: str, kb: types.InlineKeyboardMarkup, *, parse_mode=None):
        msg_id = st.get("msg_id")
        if msg_id:
            ok = await safe_edit_message(bot, chat_id, msg_id, text, reply_markup=kb, parse_mode=parse_mode)
            if ok:
                return
            try:
                await bot.delete_message(chat_id, msg_id)
            except TelegramBadRequest as e:
                bot_logger.debug(
                    "edit_ui: failed to delete old message",
                    extra={"chat_id": chat_id, "message_id": msg_id, "error": str(e)},
                )
            except Exception:
                bot_logger.debug(
                    "edit_ui: unexpected delete error",
                    extra={"chat_id": chat_id, "message_id": msg_id},
                )
        sent = await bot.send_message(chat_id, text, reply_markup=kb, parse_mode=parse_mode)
        st["msg_id"] = sent.message_id
        UI[chat_id] = st
        bot_logger.info(
            "edit_ui: sent new ui message",
            extra={"chat_id": chat_id, "message_id": st["msg_id"]},
        )

    async def update_ui_from_callback(q: types.CallbackQuery, st: dict, text: str, kb: types.InlineKeyboardMarkup, *, parse_mode=None):
        if not q.message:
            bot_logger.warning("update_ui_from_callback without message", extra={"user": describe_user(q)})
            return
        ok = await safe_edit(q.message, text, reply_markup=kb, parse_mode=parse_mode)
        if ok:
            return
        chat_id = q.message.chat.id
        old_id = st.get("msg_id")
        sent = await q.message.answer(text, reply_markup=kb, parse_mode=parse_mode)
        st["msg_id"] = sent.message_id
        UI[chat_id] = st
        bot_logger.info(
            "update_ui_from_callback: replaced ui message",
            extra={"chat_id": chat_id, "old_message_id": old_id, "message_id": st["msg_id"]},
        )
        if old_id and old_id != sent.message_id:
            try:
                await q.message.bot.delete_message(chat_id, old_id)
            except TelegramBadRequest as e:
                bot_logger.debug(
                    "update_ui_from_callback: delete failed",
                    extra={"chat_id": chat_id, "message_id": old_id, "error": str(e)},
                )
            except Exception:
                bot_logger.debug(
                    "update_ui_from_callback: unexpected delete error",
                    extra={"chat_id": chat_id, "message_id": old_id},
                )

    @DP.message(Command("start"))
    @only_owner
    @bot_action("command:/start")
    async def h_start(m: types.Message):
        _, st = await ensure_ui_message(m)
        st["selected"] = None
        await edit_ui(m.bot, m.chat.id, st, "Выберите сервер:", build_nodes_page(st["page"]))

    @DP.message(Command("nodes"))
    @only_owner
    @bot_action("command:/nodes")
    async def h_nodes(m: types.Message):
        _, st = await ensure_ui_message(m)
        st["selected"] = None
        await edit_ui(m.bot, m.chat.id, st, "Выберите сервер:", build_nodes_page(st["page"]))

    # --- обработка всех кнопок ---
    @DP.callback_query(F.data == "noop")
    @only_owner
    @bot_action("callback:noop")
    async def cb_noop(q: types.CallbackQuery):
        await q.answer()

    @DP.callback_query(F.data.startswith("page:"))
    @only_owner
    @bot_action("callback:page")
    async def cb_page(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        try:
            page = int(q.data.split(":", 1)[1])
        except (IndexError, ValueError):
            bot_logger.warning("failed to parse page", extra={"data": q.data})
            await q.answer("Ошибка страницы", show_alert=True)
            return
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        st["page"] = page
        st["selected"] = None
        UI[chat_id] = st
        await update_ui_from_callback(q, st, "Выберите сервер:", build_nodes_page(page))
        await q.answer()

    @DP.callback_query(F.data.startswith("server:"))
    @only_owner
    @bot_action("callback:server")
    async def cb_server(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        try:
            name = q.data.split(":", 1)[1]
        except IndexError:
            await q.answer("Ошибка выбора", show_alert=True)
            return
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        st["selected"] = name
        UI[chat_id] = st
        title, _, _, _, _ = describe_server(name)
        await update_ui_from_callback(q, st, title, build_server_menu(name), parse_mode="Markdown")
        await q.answer()

    @DP.callback_query(F.data.startswith("action:stats:"))
    @only_owner
    @bot_action("callback:stats")
    async def cb_stats(q: types.CallbackQuery):
        if not q.message:
            await q.answer("Нет сообщения", show_alert=True)
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, addr, status, _ = describe_server(target)
        if status != "alive" or not addr:
            bot_logger.warning("stats: server offline or address missing", extra={"server": target, "status": status, "addr": addr})
            await update_ui_from_callback(
                q,
                st,
                format_server_title(target, status, False),
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Сервер оффлайн", show_alert=True)
            return
        rpc_logger.info("RPC GetStats request", extra={"server": target, "addr": addr})
        started = time.time()
        res = await call_rpc(addr, "GetStats", {"target": target})
        duration_ms = int((time.time() - started) * 1000)
        if res.get("ok"):
            rpc_logger.info(
                "RPC GetStats ok",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms},
            )
            s = res["stats"]
            cpu_info = ", ".join(f"{x}%" for x in s['cpu_per_core_pct'])
            text = (
                f"{title}\n\n"
                f"Uptime: {s['uptime_s']}s\n"
                f"CPU: {cpu_info}\n"
                f"RAM: {s['ram']['used_mb']}/{s['ram']['total_mb']} MB ({s['ram']['pct']}%)\n"
                f"Disk /: {s['disk_root']['used_gb']}/{s['disk_root']['total_gb']} GB ({s['disk_root']['pct']}%)"
            )
            await update_ui_from_callback(q, st, text, build_server_menu(target), parse_mode="Markdown")
            await q.answer()
        else:
            err = res.get("error")
            rpc_logger.error(
                "RPC GetStats failed",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms, "error": err},
            )
            friendly = friendly_error_message(err)
            await update_ui_from_callback(
                q,
                st,
                f"{title}\nОшибка получения статуса: {friendly}",
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer(f"Ошибка: {friendly}", show_alert=True)

    @DP.callback_query(F.data.startswith("action:reboot:"))
    @only_owner
    @bot_action("callback:reboot_confirm")
    async def cb_reboot_ask(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, addr, status, is_host = describe_server(target)
        if status != "alive" or not addr:
            bot_logger.warning(
                "reboot confirm: server offline or address missing",
                extra={"server": target, "status": status, "addr": addr},
            )
            await update_ui_from_callback(
                q,
                st,
                format_server_title(target, status, is_host),
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Сервер оффлайн", show_alert=True)
            return
        await update_ui_from_callback(
            q,
            st,
            f"{title}\n\nПерезагрузить этот сервер?",
            build_reboot_confirm(target),
            parse_mode="Markdown",
        )
        await q.answer()

    @DP.callback_query(F.data.startswith("action:reboot_back:"))
    @only_owner
    @bot_action("callback:reboot_back")
    async def cb_reboot_back(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, _, _, _ = describe_server(target)
        await update_ui_from_callback(q, st, title, build_server_menu(target), parse_mode="Markdown")
        await q.answer()

    @DP.callback_query(F.data.startswith("action:reboot_yes:"))
    @only_owner
    @bot_action("callback:reboot_yes")
    async def cb_reboot_yes(q: types.CallbackQuery):
        if not q.message:
            await q.answer("Нет сообщения", show_alert=True)
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, addr, status, is_host = describe_server(target)
        if status != "alive" or not addr:
            bot_logger.warning("reboot: server offline or address missing", extra={"server": target, "status": status, "addr": addr})
            await update_ui_from_callback(
                q,
                st,
                format_server_title(target, status, is_host),
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Сервер оффлайн", show_alert=True)
            return
        rpc_logger.info("RPC Reboot request", extra={"server": target, "addr": addr})
        started = time.time()
        res = await call_rpc(addr, "Reboot", {"target": target})
        duration_ms = int((time.time() - started) * 1000)
        if res.get("ok"):
            rpc_logger.info(
                "RPC Reboot ok",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms},
            )
            await update_ui_from_callback(
                q,
                st,
                f"{title}\nОтправлена команда перезагрузки…",
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Перезагрузка запрошена")
        else:
            err = res.get("error")
            rpc_logger.error(
                "RPC Reboot failed",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms, "error": err},
            )
            friendly = friendly_error_message(err)
            await update_ui_from_callback(
                q,
                st,
                f"{title}\nОшибка перезагрузки: {friendly}",
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer(f"Ошибка: {friendly}", show_alert=True)

    @DP.callback_query(F.data.startswith("action:net:"))
    @only_owner
    @bot_action("callback:net")
    async def cb_net(q: types.CallbackQuery):
        if not q.message:
            await q.answer("Нет сообщения", show_alert=True)
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, addr, status, is_host = describe_server(target)
        if status != "alive" or not addr:
            bot_logger.warning(
                "speedtest: server offline or address missing",
                extra={"server": target, "status": status, "addr": addr},
            )
            await update_ui_from_callback(
                q,
                st,
                format_server_title(target, status, is_host),
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Сервер оффлайн", show_alert=True)
            return
        await update_ui_from_callback(
            q,
            st,
            f"{title}\nВыполняю спидтест…",
            build_server_menu(target),
            parse_mode="Markdown",
        )
        rpc_logger.info("RPC RunSpeedtest request", extra={"server": target, "addr": addr})
        started = time.time()
        res = await rpc_speedtest(addr)
        duration_ms = int((time.time() - started) * 1000)
        if res.get("ok"):
            rpc_logger.info(
                "RPC RunSpeedtest ok",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms, "down": res.get("down_mbps"), "up": res.get("up_mbps"), "ping": res.get("ping_ms")},
            )
            text = (
                f"{title}\n"
                f"↓ {res['down_mbps']} Mbit/s • ↑ {res['up_mbps']} Mbit/s • ping {res['ping_ms']} ms"
            )
            await update_ui_from_callback(q, st, text, build_server_menu(target), parse_mode="Markdown")
            await q.answer("Готово")
        else:
            err = res.get("error")
            rpc_logger.error(
                "RPC RunSpeedtest failed",
                extra={"server": target, "addr": addr, "duration_ms": duration_ms, "error": err},
            )
            friendly = friendly_error_message(err)
            await update_ui_from_callback(
                q,
                st,
                f"{title}\nОшибка спидтеста: {friendly}",
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer(f"Ошибка: {friendly}", show_alert=True)

    @DP.callback_query(F.data.startswith("action:metrics:"))
    @only_owner
    @bot_action("callback:metrics")
    async def cb_metrics(q: types.CallbackQuery):
        if not q.message:
            await q.answer("Нет сообщения", show_alert=True)
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, peer, addr, status, is_host = describe_server(target)
        if status != "alive" or not addr:
            bot_logger.warning(
                "metrics: server offline or address missing",
                extra={"server": target, "status": status, "addr": addr},
            )
            await update_ui_from_callback(
                q,
                st,
                format_server_title(target, status, is_host),
                build_server_menu(target),
                parse_mode="Markdown",
            )
            await q.answer("Сервер оффлайн", show_alert=True)
            return
        await update_ui_from_callback(
            q,
            st,
            f"{title}\nПолучаю историю метрик…",
            build_server_menu(target),
            parse_mode="Markdown",
        )

        cpu_started = time.time()
        rpc_logger.info("RPC GetTS(cpu) request", extra={"server": target, "addr": addr})
        cpu_res = await rpc_get_ts(addr, "cpu", hours=6)
        cpu_duration_ms = int((time.time() - cpu_started) * 1000)
        if cpu_res.get("ok"):
            rpc_logger.info(
                "RPC GetTS(cpu) ok",
                extra={"server": target, "addr": addr, "duration_ms": cpu_duration_ms, "points": len(cpu_res.get("series", []))},
            )
        else:
            rpc_logger.error(
                "RPC GetTS(cpu) failed",
                extra={"server": target, "addr": addr, "duration_ms": cpu_duration_ms, "error": cpu_res.get("error")},
            )

        net_started = time.time()
        rpc_logger.info("RPC GetTS(net) request", extra={"server": target, "addr": addr})
        net_res = await rpc_get_ts(addr, "net", hours=6)
        net_duration_ms = int((time.time() - net_started) * 1000)
        if net_res.get("ok"):
            rpc_logger.info(
                "RPC GetTS(net) ok",
                extra={
                    "server": target,
                    "addr": addr,
                    "duration_ms": net_duration_ms,
                    "down_points": len(net_res.get("down", [])),
                    "up_points": len(net_res.get("up", [])),
                },
            )
        else:
            rpc_logger.error(
                "RPC GetTS(net) failed",
                extra={"server": target, "addr": addr, "duration_ms": net_duration_ms, "error": net_res.get("error")},
            )

        if cpu_res.get("ok"):
            cpu_count, avg_cpu, max_cpu = summarize_series_points(cpu_res.get("series", []))
            if cpu_count:
                window = sample_window_label(cpu_count, "измерение", "измерений")
                cpu_line = (
                    f"CPU recent samples ({window}): среднее {avg_cpu:.1f}% • максимум {max_cpu:.1f}%."
                )
            else:
                cpu_line = "CPU: исторические данные отсутствуют."
        else:
            cpu_line = f"CPU: ошибка {friendly_error_message(cpu_res.get('error'))}"
            cpu_count = 0
            avg_cpu = max_cpu = 0.0

        if net_res.get("ok"):
            net_count, avg_down, avg_up = summarize_net_points(
                net_res.get("down", []),
                net_res.get("up", []),
            )
            if net_count:
                window = sample_window_label(net_count, "замер", "замеров")
                net_line = (
                    f"Сеть — {window} (recent samples): средняя ↓ {avg_down:.1f} Mbit/s • "
                    f"средняя ↑ {avg_up:.1f} Mbit/s."
                )
            else:
                net_line = "Сеть: исторические данные отсутствуют."
        else:
            net_line = f"Сеть: ошибка {friendly_error_message(net_res.get('error'))}"
            net_count = 0
            avg_down = avg_up = 0.0

        bot_logger.info(
            "metrics summary ready",
            extra={
                "server": target,
                "cpu_count": cpu_count,
                "cpu_avg": round(avg_cpu, 2),
                "cpu_max": round(max_cpu, 2),
                "net_count": net_count,
                "net_avg_down": round(avg_down, 2),
                "net_avg_up": round(avg_up, 2),
            },
        )

        text = f"{title}\n\n{cpu_line}\n{net_line}"
        await update_ui_from_callback(q, st, text, build_server_menu(target), parse_mode="Markdown")
        await q.answer("Готово")

    @DP.callback_query(F.data == "back:nodes")
    @only_owner
    @bot_action("callback:back_nodes")
    async def cb_back_nodes(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        st["selected"] = None
        UI[chat_id] = st
        await update_ui_from_callback(q, st, "Выберите сервер:", build_nodes_page(st["page"]))
        await q.answer()

    @DP.callback_query(F.data.startswith("back:server:"))
    @only_owner
    @bot_action("callback:back_server")
    async def cb_back_server(q: types.CallbackQuery):
        if not q.message:
            await q.answer()
            return
        parts = q.data.split(":", 2)
        target = parts[2] if len(parts) > 2 else ""
        chat_id = q.message.chat.id
        st = ensure_ui(chat_id)
        if target:
            st["selected"] = target
        target = st.get("selected")
        UI[chat_id] = st
        if not target:
            await q.answer("Сначала выберите сервер", show_alert=True)
            return
        title, _, _, _, _ = describe_server(target)
        await update_ui_from_callback(q, st, title, build_server_menu(target), parse_mode="Markdown")
        await q.answer()

    @DP.message(Command("invite"))
    @only_owner
    @bot_action("command:/invite")
    async def cmd_invite(m: types.Message):
        parts = m.text.strip().split(maxsplit=1)
        ttl_s = 900
        if len(parts) == 2:
            arg = parts[1].strip().lower()
            if arg.endswith("s"): ttl_s = int(arg[:-1])
            elif arg.endswith("m"): ttl_s = int(arg[:-1]) * 60
            elif arg.endswith("h"): ttl_s = int(arg[:-1]) * 3600
            else:
                try: ttl_s = int(arg)
                except: pass
        tok = secrets.token_urlsafe(16)
        tokens = invites.get("tokens", [])
        tokens.append({"token": tok, "exp_ts": now_s() + ttl_s})
        invites["tokens"] = tokens
        save_json(INVITES_FILE, invites)
        bot_logger.info("invite generated", extra={"ttl_s": ttl_s, "token_prefix": tok[:6]})
        host = PUBLIC_ADDR or LISTEN_ADDR
        link = f"join://{host}?net={state.get('network_id')}&token={tok}&ttl={ttl_s}s"
        await m.reply(f"Join link (valid {ttl_s}s):\n`{link}`", parse_mode="Markdown")

    async def _run():
        global BOT_RUNNING_OWNER
        try:
            # Жёстко обрубаем любые висящие getUpdates этим токеном
            try:
                await BOT.delete_webhook(drop_pending_updates=True)
            except Exception as e:
                print(f"[bot] pre-start delete_webhook failed: {e}")
            await asyncio.sleep(1.0)

            while True:
                # Выходим, если поколение сменилось
                if my_gen != BOT_RUN_GEN:
                    print("[bot] generation changed, exiting polling loop")
                    break

                # Доп. страховка: мы всё ещё лидер и владелец lease?
                L = current_leader()
                am_leader = (L.get("node_id") == NODE_ID)
                owner, until = get_bot_lease()
                have_lease = (owner == NODE_ID and until > now_s())
                if not (am_leader and have_lease):
                    print(f"[bot] exiting: am_leader={am_leader}, have_lease={have_lease}, owner={owner[:8] if owner else ''}")
                    break

                try:
                    print(
                        f"[bot] loop: am_leader={am_leader}, have_lease={have_lease}, my_gen={my_gen}, global_gen={BOT_RUN_GEN}"
                    )
                    await DP.start_polling(BOT, allowed_updates=DP.resolve_used_update_types())
                    print("[bot] polling finished gracefully")
                    break  # если вернулось без исключения — выходим
                except Exception as e:
                    from aiogram.exceptions import TelegramConflictError
                    if isinstance(e, TelegramConflictError):
                        print(f"[bot] polling conflict: {e!s}")
                        # Проверим, не сменился ли владелец lease
                        lease_owner, lease_until = owner, until
                        try:
                            coord = lease_coordinator_peer()
                            if coord and coord.get("addr"):
                                info = await lease_get_from(coord["addr"])
                                if info.get("ok"):
                                    lease_owner = info.get("owner", lease_owner)
                                    lease_until = int(info.get("until", lease_until) or 0)
                        except Exception as le:
                            print(f"[bot] lease check failed after conflict: {le}")
                        else:
                            if lease_owner != NODE_ID:
                                print(f"[bot] conflict: lease now owned by {lease_owner[:8] if lease_owner else '<none>'}, stopping")
                                break
                            if lease_until <= now_s():
                                print("[bot] conflict: lease expired, stopping")
                                break
                        await asyncio.sleep(1.5)
                        continue
                    else:
                        print(f"[bot] polling error: {e!r}")
                        await asyncio.sleep(1.5)
                        continue
        except asyncio.CancelledError:
            print("[bot] polling task cancelled")
        finally:
            # финальная зачистка — рубим webhook и закрываем сессии
            try:
                await BOT.delete_webhook(drop_pending_updates=True)
            except Exception as e:
                print(f"[bot] cleanup webhook error: {e}")
            try:
                await DP.stop_polling()
            except Exception as e:
                print(f"[bot] cleanup stop_polling error: {e}")
            try:
                await BOT.session.close()
            except Exception as e:
                print(f"[bot] cleanup session close error: {e}")
            BOT_RUNNING_OWNER = None
    # ВАЖНО: создаём фоновой таск
    BOT_TASK = asyncio.create_task(_run())

async def stop_bot():
    """Stop polling and clean up bot resources safely."""
    global BOT, DP, BOT_TASK, BOT_RUN_GEN, BOT_RUNNING_OWNER, BOT_LAST_BROADCAST_UNTIL, BOT_LAST_BROADCAST_OWNER

    async with BOT_LOCK:
        if not bot_task_running() and BOT is None and DP is None:
            BOT_RUNNING_OWNER = None
            return

        # 0) мгновенно «инвалидируем» активный цикл
        BOT_RUN_GEN += 1

        # 1) Просим polling завершиться корректно и ждём таск
        try:
            if DP is not None:
                print("[bot] stop: DP.stop_polling() sent")
                await DP.stop_polling()
        except Exception as e:
            print(f"[bot] stop: DP.stop_polling error: {e}")
        task = BOT_TASK
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # 2) Убираем webhook — следующий лидер начнёт polling без конфликта
        try:
            from aiogram import Bot as _Bot2
            _tmp2 = _Bot2(BOT_TOKEN)
            try:
                await _tmp2.delete_webhook(drop_pending_updates=True)
                print("[bot] stop: delete_webhook OK")
            except Exception as e:
                print(f"[bot] stop: delete_webhook failed: {e}")
            finally:
                await _tmp2.session.close()
        except Exception as e:
            print(f"[bot] stop: delete_webhook error: {e}")

        BOT_TASK = None
        DP = None
        BOT = None
        BOT_RUNNING_OWNER = None
        BOT_LAST_BROADCAST_UNTIL = 0
        BOT_LAST_BROADCAST_OWNER = None

async def leader_watcher():
    """Track leadership status and uphold the Telegram polling lease."""
    was_leader = False
    grace_deadline = 0.0
    while True:
        try:
            L = current_leader()
        except Exception as e:
            print(f"[leader] current_leader error: {e}")
            await asyncio.sleep(1.0)
            continue

        am = (L.get("node_id") == NODE_ID)
        coord = lease_coordinator_peer()
        owner, until = get_bot_lease()
        nowt = now_s()
        if coord and coord.get("addr"):
            info = await lease_get_from(coord["addr"])
            if info.get("ok"):
                owner = info.get("owner", owner)
                until = int(info.get("until", until) or 0)
                set_bot_lease(owner or "", until)

        running = bot_task_running()
        if running and (not am or owner != NODE_ID or until <= nowt):
            reasons = []
            if not am:
                reasons.append("lost leadership")
            if owner != NODE_ID:
                reasons.append(f"lease -> {owner[:8] if owner else '<none>'}")
            if until <= nowt:
                reasons.append("lease expired")
            print(f"[leader] stopping local bot due to {', '.join(reasons)}")
            await stop_bot()

        if not am:
            if was_leader:
                print(f"[leader] lost leadership to {L.get('name')} ({L.get('node_id','')[:8]})")
                if coord and coord.get("addr") and owner == NODE_ID:
                    await lease_release_from(coord["addr"], NODE_ID)
                if owner == NODE_ID:
                    set_bot_lease("", 0)
            was_leader = False
            await asyncio.sleep(1.0)
            continue

        if am and not was_leader:
            print(f"[leader] became leader: {SERVER_NAME} ({NODE_ID[:8]}); grace={LEADER_GRACE_SEC}s")
            grace_deadline = time.time() + LEADER_GRACE_SEC
            was_leader = True

        if time.time() < grace_deadline:
            await asyncio.sleep(0.5)
            continue

        if not BOT_TOKEN or not state.get("owner_username"):
            print("[leader] bot disabled (no BOT_TOKEN or owner_username)")
            await asyncio.sleep(1.0)
            continue

        if owner != NODE_ID or until <= nowt:
            previous_owner = owner
            acquired = False
            if coord and coord.get("addr"):
                got = await lease_acquire_from(coord["addr"], NODE_ID, BOT_LEASE_TTL)
                if got.get("ok"):
                    owner = got.get("owner", NODE_ID)
                    until = int(got.get("until", nowt + BOT_LEASE_TTL))
                    acquired = owner == NODE_ID
                else:
                    owner = got.get("owner", owner)
                    until = int(got.get("until", until) or 0)
                    set_bot_lease(owner, until)
                    print(f"[leader] lease denied: owner={got.get('owner','')[:8]} until={got.get('until')}")
            else:
                owner = NODE_ID
                until = nowt + BOT_LEASE_TTL
                acquired = True
            if acquired:
                print(f"[lease] acquired until {until}")
                # Broadcast takeover only if the ownership actually moved to us.
                await propagate_bot_lease(
                    NODE_ID,
                    until,
                    force_takeover=(previous_owner != NODE_ID),
                )
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(1.0)
                continue
        else:
            if until - nowt < BOT_LEASE_TTL // 2:
                refreshed = False
                if coord and coord.get("addr"):
                    got = await lease_acquire_from(coord["addr"], NODE_ID, BOT_LEASE_TTL)
                    if got.get("ok"):
                        until = int(got.get("until", until))
                        refreshed = True
                    else:
                        print(f"[lease] renew denied by {got.get('owner','')[:8]} until={got.get('until')}")
                else:
                    until = nowt + BOT_LEASE_TTL
                    refreshed = True
                if refreshed:
                    print(f"[lease] renewed until {until}")
                    # Silent refresh keeps local state fresh without re-running takeovers.
                    await propagate_bot_lease(NODE_ID, until)
                    await asyncio.sleep(0.5)

        if owner == NODE_ID and not running:
            print("[leader] starting bot (lease owner)")
            await start_bot()

        was_leader = True
        await asyncio.sleep(1.0)

def lease_coordinator_peer() -> Optional[Dict[str, Any]]:
    # координирующий узел — с минимальным node_id среди alive + self
    alive = get_alive_peers()
    # включаем себя
    my = self_peer.copy()
    my["node_id"] = NODE_ID
    alive_ids = {p.get("node_id") for p in alive}
    if NODE_ID not in alive_ids:
        alive.append(my)
    if not alive:
        return None
    best = min(alive, key=lambda p: p.get("node_id", ""))
    return best

# --- Application bootstrap -------------------------------------------------------
def parse_listen(addr: str) -> Tuple[str,int]:
    host, port = addr.split(":")
    return host, int(port)

async def on_startup(app):
    await ensure_http_client()
    # Если это init-узел, state уже должен содержать network_id/secret/owner
    # Если join — выполним присоединение
    await do_join_if_needed()
    # Обновим self_peer в state
    upsert_peer(self_peer)
    # Запускаем фоновые циклы
    app['hb'] = asyncio.create_task(heartbeat_loop())
    app['lw'] = asyncio.create_task(leader_watcher())
    app['telemetry'] = asyncio.create_task(telemetry_loop())

async def on_cleanup(app):
    app['hb'].cancel()
    app['lw'].cancel()
    app['telemetry'].cancel()
    await stop_bot()
    global http_client
    client = http_client
    http_client = None
    if client:
        await client.close()

def main():
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    host, port = parse_listen(LISTEN_ADDR)
    web.run_app(app, host=host, port=port)

if __name__ == "__main__":
    main()
