#!/usr/bin/env python3
import asyncio, json, os, hmac, hashlib, time, uuid, signal, sys, secrets
from typing import Dict, Any, List, Optional, Tuple
from aiohttp import web, ClientSession, ClientTimeout
import psutil

# Зарефакторить код

# ----------------------------
# Конфиг / состояние
# ----------------------------
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

# Секрет сети (для HMAC подписи). В init задаётся; при join — приходит от seed.
NETWORK_ID = os.environ.get("NETWORK_ID", "")
NETWORK_SECRET = os.environ.get("NETWORK_SECRET", "")

# Тайминги
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "2.0"))
DOWN_AFTER_MISSES = int(os.environ.get("DOWN_AFTER_MISSES", "3"))
RPC_TIMEOUT = float(os.environ.get("RPC_TIMEOUT", "3.0"))
CLOCK_SKEW = int(os.environ.get("CLOCK_SKEW", "15"))  # сек, допускаемая рассинхронизация в RPC

LEADER_GRACE_SEC = float(os.environ.get("LEADER_GRACE_SEC", str(DOWN_AFTER_MISSES*HEARTBEAT_INTERVAL + 2.0)))

BOT_LEASE_TTL = int(os.environ.get("BOT_LEASE_TTL", "10"))  # секунд

# Вспомогательные
def now_s() -> int: return int(time.time())

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

# Сетевое общее состояние (кэш на узле)
state = load_json(STATE_FILE, {
    "network_id": NETWORK_ID or "",
    "owner_username": OWNER_USERNAME or "",
    "network_secret": NETWORK_SECRET or "",
    "peers": [],  # [{name, addr, node_id, status, last_seen}]
    "bot_lease": {"owner": "", "until": 0}
})

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

# Локальная таблица пиров: node_id -> peer
peers: Dict[str, Dict[str, Any]] = {}
self_peer = {"name": SERVER_NAME, "addr": PUBLIC_ADDR, "node_id": NODE_ID, "status": "alive", "last_seen": now_s()}

# ----------------------------
# Подпись RPC (HMAC)
# ----------------------------
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

def set_bot_lease(owner: str, until: int):
    state["bot_lease"] = {"owner": owner, "until": until}
    save_json(STATE_FILE, state)

def get_bot_lease():
    bl = state.get("bot_lease", {}) or {}
    return bl.get("owner",""), int(bl.get("until",0))

# ----------------------------
# Вспомогательные оперции с peer-list
# ----------------------------
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

def get_alive_peers() -> List[Dict[str, Any]]:
    alive = []
    now = now_s()
    for p in [*peers.values(), self_peer]:
        last = p.get("last_seen", 0)
        misses = max(0, int((now - last) // HEARTBEAT_INTERVAL))
        status = "alive" if misses < DOWN_AFTER_MISSES else "offline"
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

# ----------------------------
# Метрики
# ----------------------------
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

# ----------------------------
# HTTP сервер (RPC)
# ----------------------------
routes = web.RouteTableDef()
http_client: Optional[ClientSession] = None

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

    else:
        return web.json_response({"ok": False, "error": "unknown method"}, status=400)

async def get_lease(addr: str):
    return await call_rpc(addr, "GetLease", {})

async def try_acquire_lease(addr: str, candidate: str, ttl: int):
    return await call_rpc(addr, "TryAcquireLease", {"candidate": candidate, "ttl": ttl})

async def release_lease(addr: str, candidate: str):
    return await call_rpc(addr, "ReleaseLease", {"candidate": candidate})

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

# ----------------------------
# Клиентские вызовы (RPC)
# ----------------------------
async def call_rpc(addr: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not state.get("network_secret"):
        return {"ok": False, "error": "no_network_secret"}
    payload = {"method": method, "params": params, "ts": now_s()}
    payload["sig"] = make_sig(payload, state["network_secret"])
    url = f"http://{addr}/rpc"
    try:
        async with http_client.post(url, json=payload, timeout=ClientTimeout(total=RPC_TIMEOUT)) as r:
            return await r.json()
    except Exception as e:
        return {"ok": False, "error": f"rpc_error:{e}"}

# ----------------------------
# Heartbeat / Discovery
# ----------------------------
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

def peer_status(p: Dict[str, Any]) -> str:
    last = int(p.get("last_seen", 0) or 0)
    misses = max(0, int((now_s() - last) // HEARTBEAT_INTERVAL))
    return "alive" if misses < DOWN_AFTER_MISSES else "offline"

def peers_with_status() -> List[Dict[str, Any]]:
    # объединяем известных пиров и себя; статусы считаем на лету
    merged = {p.get("node_id",""): dict(p) for p in state.get("peers", [])}
    merged[NODE_ID] = dict(self_peer)
    out = []
    for nid, p in merged.items():
        q = dict(p)
        q["status"] = peer_status(q)
        out.append(q)
    return out

# ----------------------------
# JOIN (если узел впервые стартует с JOIN_URL)
# ----------------------------
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


# ----------------------------
# Telegram бот (aiogram v3)
# ----------------------------
BOT_TASK: Optional[asyncio.Task] = None
def normalized_owner() -> str:
    u = state.get("owner_username","").strip()
    return u[1:] if u.startswith("@") else u

async def start_bot():
    global BOT_TASK
    if BOT_TASK is not None and not BOT_TASK.done():
        return
    from aiogram import Bot, Dispatcher, F, types
    from aiogram.filters import Command
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    owner = normalized_owner()
    def only_owner(handler):
        async def wrapper(m: types.Message, *a, **k):
            u = (m.from_user.username or "").lower()
            if u.lower() != owner.lower():
                return  # игнор других
            return await handler(m)
        return wrapper

    @dp.message(Command("start"))
    @only_owner
    async def cmd_start(m: types.Message):
        await m.answer(f"{APP_NAME} ready. Use /nodes, /stats <name>, /reboot <name>, /invite <ttl>")

    @dp.message(Command("nodes"))
    @only_owner
    async def cmd_nodes(m: types.Message):
        L = current_leader().get("node_id")
        lines = []
        # соберём последнюю копию
        lst = peers_with_status()  # вместо state["peers"]
        L = current_leader().get("node_id")
        by_id = {p.get("node_id", ""): p for p in lst}
        lines = []
        for nid, p in sorted(by_id.items(), key=lambda kv: kv[1].get("name", "")):
            name = p.get("name", nid)
            tag = " — Хост" if nid == L else ""
            if p.get("status") != "alive":
                tag += " (offline)"
            lines.append(f"• {name}{tag}")
        await m.answer("\n".join(lines) if lines else "No nodes")

    @dp.message(Command("stats"))
    @only_owner
    async def cmd_stats(m: types.Message):
        parts = m.text.strip().split(maxsplit=1)
        if len(parts) < 2:
            return await m.reply("Usage: /stats <server_name>")
        target = parts[1].strip()
        addr = None
        for p in state.get("peers", []) + [self_peer]:
            if p.get("name")==target:
                addr = p.get("addr"); break
        if target == SERVER_NAME: addr = f"{LISTEN_ADDR}"
        if not addr:
            return await m.reply("Unknown node")
        res = await call_rpc(addr, "GetStats", {"target": target})
        if not res.get("ok"):
            return await m.reply(f"Error: {res.get('error')}")
        s = res["stats"]
        msg = (f"*{s['server_name']}*\n"
               f"Uptime: {s['uptime_s']}s\n"
               f"CPU: {', '.join(str(x)+'%' for x in s['cpu_per_core_pct'])}\n"
               f"RAM: {s['ram']['used_mb']}/{s['ram']['total_mb']} MB ({s['ram']['pct']}%)\n"
               f"Disk /: {s['disk_root']['used_gb']}/{s['disk_root']['total_gb']} GB ({s['disk_root']['pct']}%)")
        await m.reply(msg, parse_mode="Markdown")

    @dp.message(Command("reboot"))
    @only_owner
    async def cmd_reboot(m: types.Message):
        parts = m.text.strip().split(maxsplit=1)
        if len(parts) < 2:
            return await m.reply("Usage: /reboot <server_name>")
        target = parts[1].strip()
        addr = None
        for p in state.get("peers", []) + [self_peer]:
            if p.get("name")==target:
                addr = p.get("addr"); break
        if target == SERVER_NAME: addr = f"{LISTEN_ADDR}"
        if not addr:
            return await m.reply("Unknown node")
        res = await call_rpc(addr, "Reboot", {"target": target})
        if not res.get("ok"):
            return await m.reply(f"Error: {res.get('error')}")
        await m.reply(f"Rebooting {target}…")

    @dp.message(Command("invite"))
    @only_owner
    async def cmd_invite(m: types.Message):
        parts = m.text.strip().split(maxsplit=1)
        ttl_s = 900  # 15m по умолчанию
        if len(parts) == 2:
            arg = parts[1].strip().lower()
            # простейший парсер: 30s, 15m, 2h
            if arg.endswith("s"): ttl_s = int(arg[:-1])
            elif arg.endswith("m"): ttl_s = int(arg[:-1]) * 60
            elif arg.endswith("h"): ttl_s = int(arg[:-1]) * 3600
            else:
                try: ttl_s = int(arg)
                except: pass
        tok = secrets.token_urlsafe(16)
        inv = {"token": tok, "exp_ts": now_s() + ttl_s}
        tokens = invites.get("tokens", [])
        tokens.append(inv)
        invites["tokens"] = tokens
        save_json(INVITES_FILE, invites)
        host = PUBLIC_ADDR or LISTEN_ADDR
        link = f"join://{host}?net={state.get('network_id')}&token={tok}&ttl={ttl_s}s"
        await m.reply(f"Join link (valid {ttl_s}s):\n`{link}`", parse_mode="Markdown")

    async def _run():
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await bot.session.close()
            except Exception:
                pass

    BOT_TASK = asyncio.create_task(_run())

async def stop_bot():
    global BOT_TASK
    if BOT_TASK and not BOT_TASK.done():
        BOT_TASK.cancel()
        try:
            await BOT_TASK
        except asyncio.CancelledError:
            pass
    BOT_TASK = None

async def leader_watcher():
    was_leader = False
    while True:
        L = current_leader()
        am = (L.get("node_id") == NODE_ID)

        # === Мы стали лидером (переход из non-leader -> leader)
        if am and not was_leader:
            print(f"[leader] became leader: {SERVER_NAME} ({NODE_ID[:8]}); grace={LEADER_GRACE_SEC}s")

            # Подождать grace-время, чтобы старый лидер успел остановить polling
            t0 = time.time()
            while time.time() - t0 < LEADER_GRACE_SEC:
                # если за время ожидания лидерство ушло — выходим без старта бота
                if current_leader().get("node_id") != NODE_ID:
                    break
                await asyncio.sleep(0.5)
            else:
                # grace-окно прошло и мы всё ещё лидер
                if BOT_TOKEN and state.get("owner_username"):
                    # 1) Проверяем активный lease у других
                    alive = get_alive_peers()
                    targets = sorted({p.get("addr") for p in alive if p.get("addr")})
                    for addr in targets:
                        r = await get_lease(addr)
                        if r.get("ok") and r.get("owner") and r.get("owner") != NODE_ID and int(r.get("until", 0)) > now_s():
                            print(f"[lease] another owner active at {addr}: {r.get('owner')[:8]} until {r.get('until')}")
                            break
                    else:
                        # 2) Пытаемся захватить lease у всех доступных пиров
                        ok_count = 0
                        for addr in targets:
                            r = await try_acquire_lease(addr, NODE_ID, BOT_LEASE_TTL)
                            if r.get("ok"):
                                ok_count += 1
                            else:
                                print(f"[lease] denied by {addr}: owner={r.get('owner','')[:8]} until={r.get('until')}")
                        # 3) Локальный lease как доп. защита
                        set_bot_lease(NODE_ID, now_s() + BOT_LEASE_TTL)

                        if ok_count or len(targets) == 0:
                            print("[leader] starting bot (lease acquired)")
                            await start_bot()
                        else:
                            print("[leader] lease acquire failed across peers; will retry later")
                else:
                    print("[leader] bot disabled (no BOT_TOKEN or owner_username)")

        # === Мы потеряли лидерство (переход leader -> non-leader)
        if (not am) and was_leader:
            print(f"[leader] lost leadership to {L.get('name')} ({L.get('node_id','')[:8]})")
            print("[leader] stopping bot")
            await stop_bot()
            # Освобождаем lease в сети и локально
            for p in get_alive_peers():
                addr = p.get("addr")
                if addr:
                    await release_lease(addr, NODE_ID)
            set_bot_lease("", 0)

        # === Продление lease, если мы лидер и lease скоро истекает
        owner, until = get_bot_lease()
        if am and owner == NODE_ID and until - now_s() < BOT_LEASE_TTL // 2:
            for p in get_alive_peers():
                addr = p.get("addr")
                if not addr:
                    continue
                await try_acquire_lease(addr, NODE_ID, BOT_LEASE_TTL)
            set_bot_lease(NODE_ID, now_s() + BOT_LEASE_TTL)

        was_leader = am
        await asyncio.sleep(1.0)

# ----------------------------
# HTTP сервер bootstrap
# ----------------------------
def parse_listen(addr: str) -> Tuple[str,int]:
    host, port = addr.split(":")
    return host, int(port)

async def on_startup(app):
    global http_client
    http_client = ClientSession()
    # Если это init-узел, state уже должен содержать network_id/secret/owner
    # Если join — выполним присоединение
    await do_join_if_needed()
    # Обновим self_peer в state
    upsert_peer(self_peer)
    # Запускаем фоновые циклы
    app['hb'] = asyncio.create_task(heartbeat_loop())
    app['lw'] = asyncio.create_task(leader_watcher())

async def on_cleanup(app):
    app['hb'].cancel()
    app['lw'].cancel()
    await stop_bot()
    if http_client:
        await http_client.close()

def main():
    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    host, port = parse_listen(LISTEN_ADDR)
    web.run_app(app, host=host, port=port)

if __name__ == "__main__":
    main()
