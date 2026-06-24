"""
Web UI server for TuyaMQTT bridge.
Serves a single-page app on http://localhost:8080

Endpoints:
  GET  /                  → index.html
  GET  /api/status        → JSON health snapshot
  GET  /api/registry      → parsed devices_registry.json
  GET  /api/logs          → last N log lines
  GET  /api/env           → .env keys (values redacted by default)
  POST /api/env           → save .env
  POST /api/discover      → run discover.py, stream output as SSE
  GET  /events            → SSE stream: device state + log lines
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

from aiohttp import web
from dotenv import dotenv_values, set_key

import bridge as b

log = logging.getLogger("web")

REGISTRY_FILE = Path("devices_registry.json")
ENV_FILE      = Path(".env")
WEB_PORT      = int(os.getenv("WEB_PORT", "47090"))
LOG_BUFFER    = 500   # lines kept in memory for the UI

# ── In-memory log capture ─────────────────────────────────────────────────────

_log_lines: deque[str] = deque(maxlen=LOG_BUFFER)
_sse_queues: list[asyncio.Queue] = []


class _UILogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        line = self.format(record)
        _log_lines.append(line)
        msg = json.dumps({"type": "log", "line": line})
        for q in list(_sse_queues):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass


def install_log_handler():
    h = _UILogHandler()
    h.setFormatter(logging.Formatter(
        fmt="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    logging.getLogger().addHandler(h)


def _push_state(dev_id: str):
    """Push a device state SSE event to all connected browsers."""
    worker = b._workers.get(dev_id)
    state  = b._state_cache.get(dev_id, {})
    if not worker:
        return
    msg = json.dumps({
        "type":      "state",
        "id":        dev_id,
        "name":      worker.dev.get("name"),
        "connected": worker.connected,
        "state":     state,
    })
    for q in list(_sse_queues):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


# ── API handlers ──────────────────────────────────────────────────────────────

async def handle_index(request):
    html = (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
    return web.Response(text=html, content_type="text/html")


async def handle_status(request):
    now = time.time()
    start = getattr(b, "_start_time", now)
    devices = []
    for dev_id, worker in b._workers.items():
        devices.append({
            "id":         dev_id,
            "name":       worker.dev.get("name"),
            "type":       worker.dev.get("type", "unknown"),
            "ip":         worker.dev.get("ip"),
            "version":    worker.dev.get("version"),
            "connected":  worker.connected,
            "fail_count": worker.fail_count,
            "last_seen":  round(now - worker.last_seen, 1) if worker.last_seen else None,
            "last_cmd":   round(now - worker.last_cmd,  1) if worker.last_cmd  else None,
            "state":      b._state_cache.get(dev_id, {}),
        })
    online = sum(1 for d in devices if d["connected"])
    return web.json_response({
        "uptime_s":       round(now - start),
        "devices_total":  len(devices),
        "devices_online": online,
        "devices_offline": len(devices) - online,
        "devices":        sorted(devices, key=lambda d: d["name"] or ""),
    })


async def handle_registry(request):
    if not REGISTRY_FILE.exists():
        return web.json_response({"devices": []})
    data = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
    return web.json_response(data)


async def handle_logs(request):
    n = int(request.rel_url.query.get("n", 200))
    lines = list(_log_lines)[-n:]
    return web.json_response({"lines": lines})


async def handle_env_get(request):
    vals = dotenv_values(ENV_FILE) if ENV_FILE.exists() else {}
    show = request.rel_url.query.get("show") == "1"
    result = {}
    for k, v in vals.items():
        secret = any(x in k.upper() for x in ("SECRET", "PASSWORD", "KEY"))
        result[k] = v if (show or not secret) else "••••••••"
    return web.json_response(result)


async def handle_env_post(request):
    body = await request.json()
    ENV_FILE.touch(exist_ok=True)
    for k, v in body.items():
        if isinstance(v, str) and "•" not in v:  # skip redacted placeholders
            set_key(str(ENV_FILE), k, v)
    return web.json_response({"ok": True})


async def handle_command(request):
    """Send a DPS command to a device worker."""
    body = await request.json()
    dev_id   = body.get("dev_id")
    dps_code = str(body.get("dps_code"))
    raw      = str(body.get("value"))

    worker = b._workers.get(dev_id)
    if not worker:
        return web.json_response({"ok": False, "error": "device not found"}, status=404)

    dev   = worker.dev
    value = b.parse_command(raw, dps_code, dev)
    worker.send_command(dps_code, value)
    log.info("WebUI command %s DPS%s = %s", dev.get("name"), dps_code, value)

    # push updated state hint immediately so UI feels instant
    await asyncio.sleep(0.05)
    _push_state(dev_id)
    return web.json_response({"ok": True})


async def handle_discover(request):
    """Run discover.py and stream output as SSE."""
    resp = web.StreamResponse(headers={
        "Content-Type":  "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)

    async def send(line: str):
        data = json.dumps({"line": line})
        await resp.write(f"data: {data}\n\n".encode())

    await send("Starting discovery...")
    try:
        python = sys.executable
        proc = await asyncio.create_subprocess_exec(
            python, "discover.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(Path(__file__).parent),
        )
        async for raw in proc.stdout:
            await send(raw.decode(errors="replace").rstrip())
        await proc.wait()
        code = proc.returncode
        await send(f"Discovery finished (exit {code}).")
    except Exception as e:
        await send(f"Error: {e}")

    await resp.write(b"data: {\"done\": true}\n\n")
    return resp


async def handle_sse(request):
    """Push device state + log lines to the browser in real time."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _sse_queues.append(q)

    resp = web.StreamResponse(headers={
        "Content-Type":  "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)

    # send current state snapshot on connect
    for dev_id in b._workers:
        _push_state(dev_id)

    try:
        while True:
            msg = await asyncio.wait_for(q.get(), timeout=25)
            await resp.write(f"data: {msg}\n\n".encode())
    except (asyncio.TimeoutError, ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        _sse_queues.remove(q)

    return resp


# ── App factory ───────────────────────────────────────────────────────────────

def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",              handle_index)
    app.router.add_get("/api/status",    handle_status)
    app.router.add_get("/api/registry",  handle_registry)
    app.router.add_get("/api/logs",      handle_logs)
    app.router.add_get("/api/env",       handle_env_get)
    app.router.add_post("/api/env",      handle_env_post)
    app.router.add_post("/api/command",  handle_command)
    app.router.add_get("/api/discover",  handle_discover)
    app.router.add_get("/events",        handle_sse)
    return app


async def start_web(app: web.Application):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
    await site.start()
    log.info("Web UI available at http://localhost:%d", WEB_PORT)
    return runner
