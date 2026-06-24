"""
MQTT bridge: manages persistent tinytuya device connections and
publishes Home Assistant MQTT Discovery configs.

Architecture:
  - One background thread per device (tinytuya blocking I/O)
  - Threads push state updates into an asyncio queue
  - Main asyncio loop reads queue → publishes to MQTT state topics
  - HA commands arrive via MQTT → forwarded to device threads

Run:
  uv run python bridge.py
"""

import asyncio
import json
import logging
import os
import queue
import sys
import threading
import time
from pathlib import Path

import aiomqtt
import tinytuya
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("bridge")

REGISTRY_FILE   = Path("devices_registry.json")
HA_DISCOVERY    = "homeassistant"
HEARTBEAT_SECS  = 20
RECONNECT_SECS  = 10
VERSIONS        = [3.3, 3.4, 3.5, 3.1]


# ── MQTT topic helpers ────────────────────────────────────────────────────────

def state_topic(dev_id: str)         -> str: return f"tuya/state/{dev_id}"
def avail_topic(dev_id: str)         -> str: return f"tuya/availability/{dev_id}"
def cmd_topic(dev_id: str, dps: str) -> str: return f"tuya/command/{dev_id}/{dps}"


# ── Device worker (runs in a thread) ─────────────────────────────────────────

class DeviceWorker(threading.Thread):
    """
    Maintains a persistent tinytuya connection for one device.
    Pushes state dicts into `state_queue` for the asyncio loop to publish.
    Accepts command dicts from `cmd_queue` to send to the device.
    """

    def __init__(self, dev: dict, state_queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        super().__init__(daemon=True, name=f"dev-{dev['name']}")
        self.dev         = dev
        self.state_queue = state_queue
        self.loop        = loop
        self.cmd_queue: queue.Queue = queue.Queue()
        self._stop_evt   = threading.Event()

    def stop(self):
        self._stop_evt.set()

    def send_command(self, dps_code: str, value):
        self.cmd_queue.put((dps_code, value))

    def _connect(self) -> tinytuya.Device | None:
        ip  = self.dev.get("ip", "")
        key = self.dev.get("key", "")
        ver = float(self.dev.get("version", 3.3))
        if not ip or not key:
            return None

        versions = [ver] + [v for v in VERSIONS if v != ver]
        for v in versions:
            try:
                d = tinytuya.Device(
                    dev_id=self.dev["id"],
                    address=ip,
                    local_key=key,
                    version=v,
                    persist=True,
                )
                d.set_socketTimeout(HEARTBEAT_SECS + 5)
                status = d.status()
                if status and "dps" in status:
                    self.dev["version"] = v  # remember working version
                    return d
            except Exception:
                pass
        return None

    def _push_state(self, dps: dict, available: bool = True):
        async def _pub():
            await self.state_queue.put({
                "id":        self.dev["id"],
                "dps":       dps,
                "available": available,
            })
        asyncio.run_coroutine_threadsafe(_pub(), self.loop)

    def run(self):
        name = self.dev["name"]
        while not self._stop_evt.is_set():
            log.info("[%s] Connecting...", name)
            d = self._connect()

            if d is None:
                log.warning("[%s] Unreachable — retry in %ss", name, RECONNECT_SECS)
                self._push_state({}, available=False)
                self._stop_evt.wait(RECONNECT_SECS)
                continue

            log.info("[%s] Connected (v%s)", name, self.dev.get("version"))

            # push initial status
            try:
                status = d.status()
                if status and "dps" in status:
                    self._push_state(status["dps"], available=True)
            except Exception:
                pass

            last_heartbeat = time.time()

            while not self._stop_evt.is_set():
                # ── send any pending commands ────────────────────────────
                while not self.cmd_queue.empty():
                    try:
                        dps_code, value = self.cmd_queue.get_nowait()
                        d.set_value(dps_code, value)
                        log.debug("[%s] Sent DPS%s = %s", name, dps_code, value)
                    except Exception as e:
                        log.warning("[%s] Command failed: %s", name, e)

                # ── heartbeat ────────────────────────────────────────────
                if time.time() - last_heartbeat >= HEARTBEAT_SECS:
                    try:
                        d.heartbeat(nowait=True)
                        last_heartbeat = time.time()
                    except Exception:
                        break

                # ── receive update ───────────────────────────────────────
                try:
                    data = d.receive()
                    if data is None:
                        continue
                    if "dps" in data:
                        self._push_state(data["dps"], available=True)
                    elif data.get("Err"):
                        log.warning("[%s] Device error: %s", name, data.get("Error", data))
                        break
                except Exception as e:
                    log.warning("[%s] Receive error: %s", name, e)
                    break

            log.warning("[%s] Disconnected — reconnecting in %ss", name, RECONNECT_SECS)
            self._push_state({}, available=False)
            try:
                d.close()
            except Exception:
                pass
            self._stop_evt.wait(RECONNECT_SECS)


# ── HA entity builders ────────────────────────────────────────────────────────

def ha_device_block(dev: dict) -> dict:
    return {
        "identifiers":  [dev["id"]],
        "name":          dev["name"],
        "manufacturer": "Tuya",
        "model":         dev.get("category", ""),
    }


def entities_for_device(dev: dict) -> list[dict]:
    dev_id   = dev["id"]
    name     = dev["name"]
    dps_map  = dev.get("dps_map", {})
    dev_type = dev.get("type", "unknown")
    ha_dev   = ha_device_block(dev)
    entities = []

    if dev_type in ("switch", "unknown"):
        bool_dps = sorted(
            [c for c, info in dps_map.items()
             if c.isdigit() and info.get("type") == "bool" and int(c) <= 10],
            key=int,
        )
        for i, dps_code in enumerate(bool_dps):
            suffix = f" {i + 1}" if len(bool_dps) > 1 else ""
            uid = f"{dev_id}_sw{dps_code}"
            entities.append({
                "ha_type":         "switch",
                "unique_id":       uid,
                "dps_code":        dps_code,
                "discovery_topic": f"{HA_DISCOVERY}/switch/{uid}/config",
                "config": {
                    "name":               f"{name}{suffix}",
                    "unique_id":          uid,
                    "state_topic":        state_topic(dev_id),
                    "value_template":     f"{{% set v = value_json.get('{dps_code}') %}}"
                                          "{{ 'ON' if v else 'OFF' }}",
                    "command_topic":      cmd_topic(dev_id, dps_code),
                    "payload_on":         "ON",
                    "payload_off":        "OFF",
                    "availability_topic": avail_topic(dev_id),
                    "device":             ha_dev,
                },
            })

        energy = {
            "18": ("Current", "mA",  "current", "measurement"),
            "19": ("Power",   "W",   "power",   "measurement"),
            "20": ("Voltage", "V",   "voltage", "measurement"),
        }
        for dps_code, (label, unit, dev_class, state_class) in energy.items():
            if dps_code in dps_map and dps_code.isdigit():
                uid = f"{dev_id}_e{dps_code}"
                entities.append({
                    "ha_type":         "sensor",
                    "unique_id":       uid,
                    "dps_code":        dps_code,
                    "discovery_topic": f"{HA_DISCOVERY}/sensor/{uid}/config",
                    "config": {
                        "name":                f"{name} {label}",
                        "unique_id":           uid,
                        "state_topic":         state_topic(dev_id),
                        "value_template":      f"{{{{ value_json.get('{dps_code}', 0) }}}}",
                        "unit_of_measurement": unit,
                        "device_class":        dev_class,
                        "state_class":         state_class,
                        "availability_topic":  avail_topic(dev_id),
                        "device":              ha_dev,
                    },
                })

    elif dev_type == "fan":
        if "1" in dps_map:
            uid = f"{dev_id}_fan"
            speed_dps = next(
                (c for c in sorted((k for k in dps_map if k.isdigit()), key=int)
                 if c != "1" and int(c) <= 10
                 and dps_map[c].get("type") in ("int", "str")),
                None,
            )
            config = {
                "name":               name,
                "unique_id":          uid,
                "state_topic":        state_topic(dev_id),
                "state_value_template": "{% set v = value_json.get('1') %}"
                                        "{{ 'ON' if v else 'OFF' }}",
                "command_topic":      cmd_topic(dev_id, "1"),
                "payload_on":         "ON",
                "payload_off":        "OFF",
                "availability_topic": avail_topic(dev_id),
                "device":             ha_dev,
            }
            if speed_dps and isinstance(dps_map[speed_dps].get("value"), int):
                config["percentage_state_topic"]    = state_topic(dev_id)
                config["percentage_value_template"] = f"{{{{ value_json.get('{speed_dps}', 0) }}}}"
                config["percentage_command_topic"]  = cmd_topic(dev_id, speed_dps)
                config["speed_range_min"] = 1
                config["speed_range_max"] = 100
            entities.append({
                "ha_type":         "fan",
                "unique_id":       uid,
                "dps_code":        "1",
                "discovery_topic": f"{HA_DISCOVERY}/fan/{uid}/config",
                "config":          config,
            })

    elif dev_type == "light":
        onoff_dps = "20" if "20" in dps_map else "1" if "1" in dps_map else None
        if onoff_dps:
            uid = f"{dev_id}_light"
            config = {
                "name":               name,
                "unique_id":          uid,
                "state_topic":        state_topic(dev_id),
                "state_value_template": f"{{% set v = value_json.get('{onoff_dps}') %}}"
                                        "{{ 'ON' if v else 'OFF' }}",
                "command_topic":      cmd_topic(dev_id, onoff_dps),
                "payload_on":         "ON",
                "payload_off":        "OFF",
                "availability_topic": avail_topic(dev_id),
                "device":             ha_dev,
            }
            if "22" in dps_map:
                config["brightness_state_topic"]    = state_topic(dev_id)
                config["brightness_value_template"] = (
                    "{{ ((value_json.get('22', 10) - 10) / 990 * 255) | int }}"
                )
                config["brightness_command_topic"]  = cmd_topic(dev_id, "22")
                config["brightness_scale"]          = 255
                config["on_command_type"]           = "brightness"
            if "23" in dps_map:
                config["color_temp_state_topic"]    = state_topic(dev_id)
                config["color_temp_value_template"] = (
                    "{{ (153 + value_json.get('23', 0) / 1000 * 347) | int }}"
                )
                config["color_temp_command_topic"]  = cmd_topic(dev_id, "23")
            entities.append({
                "ha_type":         "light",
                "unique_id":       uid,
                "dps_code":        onoff_dps,
                "discovery_topic": f"{HA_DISCOVERY}/light/{uid}/config",
                "config":          config,
            })

    elif dev_type == "sensor":
        for dps_code, info in sorted(((k, v) for k, v in dps_map.items() if k.isdigit()), key=lambda x: int(x[0])):
            uid = f"{dev_id}_s{dps_code}"
            entities.append({
                "ha_type":         "sensor",
                "unique_id":       uid,
                "dps_code":        dps_code,
                "discovery_topic": f"{HA_DISCOVERY}/sensor/{uid}/config",
                "config": {
                    "name":               f"{name} DPS{dps_code}",
                    "unique_id":          uid,
                    "state_topic":        state_topic(dev_id),
                    "value_template":     f"{{{{ value_json.get('{dps_code}') }}}}",
                    "availability_topic": avail_topic(dev_id),
                    "device":             ha_dev,
                },
            })

    elif dev_type == "ir":
        log.info("Skipping HA entity for IR device: %s", name)

    return entities


# ── Command value parser ──────────────────────────────────────────────────────

def parse_command(raw: str, dps_code: str, dev: dict):
    dps_type = dev.get("dps_map", {}).get(dps_code, {}).get("type", "unknown")
    if raw in ("ON", "on"):
        return True  if dps_type == "bool" else 1
    if raw in ("OFF", "off"):
        return False if dps_type == "bool" else 0
    try:
        if dps_type == "int":
            val = int(raw)
            if dps_code == "22":  # brightness: HA 0-255 → Tuya 10-1000
                return max(10, int(val / 255 * 990) + 10)
            if dps_code == "23":  # color temp: mireds → 0-1000
                return max(0, int((val - 153) / 347 * 1000))
            return val
        if dps_type == "float":
            return float(raw)
    except ValueError:
        pass
    return raw


# ── Registry ──────────────────────────────────────────────────────────────────

def load_registry() -> tuple[list[dict], float]:
    with open(REGISTRY_FILE) as f:
        data = json.load(f)
    return data.get("devices", []), REGISTRY_FILE.stat().st_mtime


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    if not REGISTRY_FILE.exists():
        log.error("devices_registry.json not found. Run: uv run python discover.py")
        sys.exit(1)

    host     = os.getenv("MQTT_HOST",     "localhost")
    port     = int(os.getenv("MQTT_PORT", "1883"))
    username = os.getenv("MQTT_USERNAME", "") or None
    password = os.getenv("MQTT_PASSWORD", "") or None

    loop         = asyncio.get_event_loop()
    state_queue: asyncio.Queue = asyncio.Queue()

    devices, registry_mtime = load_registry()
    reachable = [d for d in devices if d.get("ip") and d.get("key") and d.get("type") != "ir"]

    # start device workers
    workers: dict[str, DeviceWorker] = {}
    for dev in reachable:
        w = DeviceWorker(dev, state_queue, loop)
        w.start()
        workers[dev["id"]] = w

    log.info("Started %d device worker(s)", len(workers))

    # build entity + command map
    all_entities = []
    for dev in devices:
        all_entities.extend(entities_for_device(dev))

    cmd_map: dict[str, tuple[str, str]] = {}  # topic → (dev_id, dps_code)
    for ent in all_entities:
        cfg = ent["config"]
        dev_id = next((d["id"] for d in devices if ent["unique_id"].startswith(d["id"])), None)
        if not dev_id:
            continue
        for key in ("command_topic", "percentage_command_topic",
                    "brightness_command_topic", "color_temp_command_topic"):
            if key in cfg:
                dps = "22" if "brightness" in key else "23" if "color_temp" in key else ent["dps_code"]
                cmd_map[cfg[key]] = (dev_id, dps)

    dev_by_id = {d["id"]: d for d in devices}

    log.info("Connecting to MQTT broker at %s:%s", host, port)

    async with aiomqtt.Client(
        hostname=host,
        port=port,
        username=username,
        password=password,
        identifier="tuya-ha-bridge",
    ) as client:
        log.info("Connected to MQTT broker")

        # publish HA discovery (retained)
        for ent in all_entities:
            await client.publish(ent["discovery_topic"], json.dumps(ent["config"]), retain=True)
        log.info("Published %d HA discovery config(s)", len(all_entities))

        # mark all devices offline until we hear from them
        for dev in reachable:
            await client.publish(avail_topic(dev["id"]), "offline", retain=True)

        # subscribe to HA command topics
        for topic in cmd_map:
            await client.subscribe(topic)
        log.info("Ready — %d entity(ies), %d command topic(s)", len(all_entities), len(cmd_map))

        async def publish_states():
            """Read device state updates and publish to MQTT."""
            while True:
                update = await state_queue.get()
                dev_id    = update["id"]
                dps       = update["dps"]
                available = update["available"]

                status = "online" if available else "offline"
                await client.publish(avail_topic(dev_id), status, retain=True)

                if dps:
                    await client.publish(state_topic(dev_id), json.dumps(dps), retain=True)
                    log.debug("State %s: %s", dev_id, dps)

        async def handle_commands():
            """Receive HA commands and forward to device workers."""
            async for msg in client.messages:
                topic   = str(msg.topic)
                payload = msg.payload.decode(errors="replace")

                # hot-reload registry if changed
                nonlocal registry_mtime, devices, all_entities, cmd_map, dev_by_id
                current_mtime = REGISTRY_FILE.stat().st_mtime
                if current_mtime != registry_mtime:
                    log.info("Registry changed — reloading...")
                    devices, registry_mtime = load_registry()
                    dev_by_id = {d["id"]: d for d in devices}
                    all_entities = []
                    for dev in devices:
                        all_entities.extend(entities_for_device(dev))
                    for ent in all_entities:
                        await client.publish(ent["discovery_topic"], json.dumps(ent["config"]), retain=True)
                    new_cmds = {}
                    for ent in all_entities:
                        cfg = ent["config"]
                        dev_id = next((d["id"] for d in devices if ent["unique_id"].startswith(d["id"])), None)
                        if not dev_id:
                            continue
                        for key in ("command_topic", "percentage_command_topic",
                                    "brightness_command_topic", "color_temp_command_topic"):
                            if key in cfg:
                                dps = "22" if "brightness" in key else "23" if "color_temp" in key else ent["dps_code"]
                                new_cmds[cfg[key]] = (dev_id, dps)
                    for t in new_cmds:
                        if t not in cmd_map:
                            await client.subscribe(t)
                    cmd_map = new_cmds
                    log.info("Reload complete")

                if topic in cmd_map:
                    dev_id, dps_code = cmd_map[topic]
                    dev    = dev_by_id.get(dev_id)
                    worker = workers.get(dev_id)
                    if dev and worker:
                        value = parse_command(payload, dps_code, dev)
                        worker.send_command(dps_code, value)
                        log.info("Command %s DPS%s = %s", dev.get("name"), dps_code, value)

        await asyncio.gather(publish_states(), handle_commands())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
