"""
Main entry point — starts the full stack in order:

  1. MQTT broker        (amqtt, embedded, localhost:1883)
  2. tuya2mqtt          (subprocess, foreground mode)
  3. Bridge             (asyncio task, registers devices + HA discovery)

Usage:
  uv run python run.py

Re-discovery (without stopping the stack):
  uv run python discover.py [--force]
  (bridge detects the registry change and reloads automatically)
"""

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run")

REGISTRY_FILE  = Path("devices_registry.json")
T2M_CONF_FILE  = Path("tuya2mqtt.conf")
STARTUP_DELAY  = 2.0   # seconds to wait between each stage


# ── tuya2mqtt config ──────────────────────────────────────────────────────────

def write_t2m_config():
    """Generate tuya2mqtt.conf pointing at our embedded broker."""
    host     = os.getenv("MQTT_HOST",     "localhost")
    port     = int(os.getenv("MQTT_PORT", "1883"))
    username = os.getenv("MQTT_USERNAME", "")
    password = os.getenv("MQTT_PASSWORD", "")

    config = {
        "broker": {
            "host":     host,
            "port":     port,
            "username": username,
            "password": password,
        }
    }
    T2M_CONF_FILE.write_text(json.dumps(config, indent=2))
    log.info("Wrote %s (broker=%s:%s)", T2M_CONF_FILE, host, port)


# ── subprocess management ─────────────────────────────────────────────────────

async def start_tuya2mqtt() -> asyncio.subprocess.Process:
    """Launch tuya2mqtt in foreground mode as a managed subprocess."""
    python = sys.executable
    cmd = [python, "-m", "tuya2mqtt", "--mode", "foreground"]
    log.info("Starting tuya2mqtt: %s", " ".join(cmd))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    log.info("tuya2mqtt PID=%s", proc.pid)
    return proc


async def pipe_output(proc: asyncio.subprocess.Process, prefix: str):
    """Stream subprocess stdout to our logger."""
    if proc.stdout is None:
        return
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        log.info("[%s] %s", prefix, line.decode(errors="replace").rstrip())


async def wait_for_broker():
    """Yield control so the broker's asyncio listener fully initialises."""
    await asyncio.sleep(1.5)


# ── main ──────────────────────────────────────────────────────────────────────

async def main():
    if not REGISTRY_FILE.exists():
        log.error("devices_registry.json not found.")
        log.error("Run first:  uv run python discover.py")
        sys.exit(1)

    host = os.getenv("MQTT_HOST", "localhost")
    port = int(os.getenv("MQTT_PORT", "1883"))

    # ── Stage 1: broker ───────────────────────────────────────────────────────
    log.info("=== Stage 1: Starting MQTT broker ===")
    from broker import start_broker
    mqtt_broker = await start_broker()
    await wait_for_broker()
    log.info("Broker ready.")

    # ── Stage 2: bridge ───────────────────────────────────────────────────────
    log.info("=== Stage 2: Starting bridge ===")
    from bridge import main as bridge_main
    bridge_task = asyncio.ensure_future(bridge_main())

    log.info("=== Stack running — Ctrl+C to stop ===")

    try:
        await bridge_task
    except asyncio.CancelledError:
        pass
    finally:
        await mqtt_broker.shutdown()
        log.info("All services stopped.")


async def _watch_process(proc: asyncio.subprocess.Process):
    """Resolves when the subprocess exits — triggers stack shutdown."""
    code = await proc.wait()
    log.error("tuya2mqtt exited unexpectedly (code %s)", code)


if __name__ == "__main__":
    # Windows needs this for subprocess support
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
