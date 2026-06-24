"""
Main entry point — starts the full stack:

  1. MQTT broker   (amqtt, embedded, 0.0.0.0:1883)
  2. Bridge        (asyncio task, device workers + HA discovery)
  3. Health server (HTTP, localhost:8765)

Usage:
  uv run python run.py

Health check:
  curl http://localhost:8765/health

Re-discovery (hot-reload, no restart needed):
  uv run python discover.py [--force]
"""

import asyncio
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOGS_DIR    = Path("logs")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "47765"))


def setup_logging():
    LOGS_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # rotating file: 5 MB × 5 files
    fh = logging.handlers.RotatingFileHandler(
        LOGS_DIR / "tuyamqtt.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)

    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(ch)

    # quiet noisy libraries
    for noisy in ("transitions.core", "amqtt.broker", "amqtt.broker.plugins"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


setup_logging()
log = logging.getLogger("run")

REGISTRY_FILE = Path("devices_registry.json")
_start_time   = time.time()


# ── Health HTTP server ────────────────────────────────────────────────────────

async def health_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        await reader.read(1024)  # consume the request

        import bridge as b
        now = time.time()

        devices_status = []
        for dev_id, worker in b._workers.items():
            devices_status.append({
                "name":      worker.dev.get("name"),
                "id":        dev_id,
                "connected": worker.connected,
                "fail_count": worker.fail_count,
                "last_seen": round(now - worker.last_seen, 1) if worker.last_seen else None,
                "last_cmd":  round(now - worker.last_cmd,  1) if worker.last_cmd  else None,
            })

        online  = sum(1 for d in devices_status if d["connected"])
        offline = len(devices_status) - online

        payload = json.dumps({
            "status":        "ok",
            "uptime_s":      round(now - _start_time),
            "devices_total": len(devices_status),
            "devices_online":  online,
            "devices_offline": offline,
            "devices":       sorted(devices_status, key=lambda d: d["name"] or ""),
        }, indent=2)

        response = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "Connection: close\r\n\r\n"
            + payload
        )
        writer.write(response.encode())
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def start_health_server():
    server = await asyncio.start_server(health_handler, "127.0.0.1", HEALTH_PORT)
    log.info("Health server listening on http://localhost:%d/health", HEALTH_PORT)
    return server


# ── Graceful shutdown ─────────────────────────────────────────────────────────

def _install_signal_handlers(shutdown_event: asyncio.Event):
    loop = asyncio.get_event_loop()

    def _handle():
        log.info("Shutdown signal received")
        shutdown_event.set()

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _handle)
    # On Windows, KeyboardInterrupt from asyncio.run() covers Ctrl+C


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    if not REGISTRY_FILE.exists():
        log.error("devices_registry.json not found. Run: uv run python discover.py")
        sys.exit(1)

    shutdown_event = asyncio.Event()
    _install_signal_handlers(shutdown_event)

    # Stage 1: MQTT broker
    log.info("=== Stage 1: Starting MQTT broker ===")
    from broker import start_broker
    mqtt_broker = await start_broker()
    await asyncio.sleep(1.5)
    log.info("Broker ready.")

    # Stage 2: bridge
    log.info("=== Stage 2: Starting bridge ===")
    from bridge import main as bridge_main
    bridge_task = asyncio.ensure_future(bridge_main())

    # Stage 3: health server (lightweight JSON endpoint)
    health_server = await start_health_server()

    # Stage 4: web UI
    log.info("=== Stage 3: Starting web UI ===")
    from web import build_app, start_web, install_log_handler
    install_log_handler()
    web_runner = await start_web(build_app())

    log.info("=== Stack running - Ctrl+C to stop ===")

    # wait until shutdown signal or bridge exits unexpectedly
    done, _ = await asyncio.wait(
        [bridge_task, asyncio.ensure_future(shutdown_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    log.info("Shutting down...")
    if not bridge_task.done():
        bridge_task.cancel()
        try:
            await bridge_task
        except (asyncio.CancelledError, Exception):
            pass

    health_server.close()
    await health_server.wait_closed()
    await web_runner.cleanup()
    await mqtt_broker.shutdown()
    log.info("All services stopped.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
