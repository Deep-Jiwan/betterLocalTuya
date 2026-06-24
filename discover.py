"""
Single-script full discovery pipeline:
  1. Pull devices + local keys from Tuya Cloud
  2. Scan LAN for IPs
  3. Connect to each device, auto-detect version, probe live DPS
  4. Merge with existing registry (preserve user edits)
  5. Write devices_registry.json

Usage:
  uv run python discover.py            # skip already-probed devices
  uv run python discover.py --force    # re-probe everything
"""

import json
import os
import sys
from pathlib import Path

import tinytuya
from dotenv import load_dotenv

load_dotenv()

_DATA_DIR     = Path(os.getenv("DATA_DIR", "data"))
_DATA_DIR.mkdir(exist_ok=True)
DEVICES_JSON  = _DATA_DIR / "devices.json"
REGISTRY_FILE = _DATA_DIR / "devices_registry.json"
PROBE_TIMEOUT = 5

REGION_MAP = {"eu": "eu", "us": "us", "cn": "cn", "in": "in", "ue": "eu"}

CATEGORY_MAP = {
    "cz": "switch", "kg": "switch", "pc": "switch",
    "aqcz": "switch", "tdq": "switch",
    "fs": "fan", "fsd": "fan", "fskg": "fan",
    "dj": "light", "dd": "light", "fwd": "light", "xdd": "light", "dc": "light",
    "cl": "cover", "clkg": "cover",
    "wsdcg": "sensor", "ldcg": "sensor", "pir": "sensor",
    "ms": "sensor", "mcs": "sensor", "sj": "sensor",
    "jwbj": "sensor", "rqbj": "sensor", "ywbj": "sensor",
    "wnykq": "ir", "infrared_tv": "ir", "wfcon": "ir",
}

VERSIONS_TO_TRY = [3.3, 3.4, 3.5, 3.1]


# ---------------------------------------------------------------------------
# Step 1: Cloud fetch
# ---------------------------------------------------------------------------

def fetch_cloud_devices() -> list[dict]:
    api_key    = os.getenv("TUYA_CLIENT_ID", "").strip()
    api_secret = os.getenv("TUYA_SECRET", "").strip()
    region     = REGION_MAP.get(os.getenv("TUYA_REGION", "eu").lower(), "eu")

    if not api_key or not api_secret:
        print("ERROR: TUYA_CLIENT_ID and TUYA_SECRET must be set in .env")
        sys.exit(1)

    print(f"[1/3] Fetching devices from Tuya Cloud (region={region})...")
    cloud = tinytuya.Cloud(
        apiRegion=region,
        apiKey=api_key,
        apiSecret=api_secret,
        apiDeviceID=None,
    )
    devices = cloud.getdevices()

    if isinstance(devices, dict) and "err" in devices:
        print(f"ERROR: {devices}")
        sys.exit(1)

    if not devices:
        print("No devices returned. Check credentials and region.")
        sys.exit(1)

    print(f"      {len(devices)} device(s) found in cloud")
    return devices


# ---------------------------------------------------------------------------
# Step 2: LAN scan
# ---------------------------------------------------------------------------

def scan_lan() -> dict[str, str]:
    print("[2/3] Scanning LAN for device IPs...")
    try:
        scan_result = tinytuya.deviceScan(verbose=False, maxretry=5, color=False)
        ip_map = {}
        for ip, info in scan_result.items():
            dev_id = info.get("gwId") or info.get("devId") or info.get("id")
            if dev_id:
                ip_map[dev_id] = ip
        print(f"      {len(ip_map)} device(s) responded on LAN")
        return ip_map
    except Exception as e:
        print(f"      LAN scan error ({e}) - IPs will be empty")
        return {}


# ---------------------------------------------------------------------------
# Step 3: Live DPS probe
# ---------------------------------------------------------------------------

def _try_connect(dev_id: str, ip: str, key: str, ver: float):
    try:
        d = tinytuya.Device(dev_id=dev_id, address=ip, local_key=key, version=ver)
        d.set_socketTimeout(PROBE_TIMEOUT)
        status = d.status()
        if status and "dps" in status:
            return d, status
        if status and status.get("Err") == "904":
            return None, None
    except Exception:
        pass
    return None, None


def probe_device(dev: dict) -> dict:
    ip  = dev.get("ip", "")
    key = dev.get("key", "")
    ver = float(dev.get("ver", dev.get("version", 3.3)))

    empty = {"reachable": False, "error": None, "status_dps": {}, "detected_dps": [], "version_used": ver}

    if not ip or not key:
        return {**empty, "error": "no IP or key"}

    versions = [ver] + [v for v in VERSIONS_TO_TRY if v != ver]
    d, status = None, None
    for v in versions:
        d, status = _try_connect(dev["id"], ip, key, v)
        if d and status:
            ver = v
            break

    if not d or not status:
        return {**empty, "error": "904 on all versions tried"}

    result = {
        "reachable":    True,
        "error":        None,
        "status_dps":   status["dps"],
        "detected_dps": [],
        "version_used": ver,
    }

    try:
        detected = d.detect_available_dps()
        if detected and "dps" in detected:
            result["detected_dps"] = sorted(int(k) for k in detected["dps"].keys())
            result["status_dps"].update(detected["dps"])
    except Exception:
        pass

    return result


def infer_type(value) -> str:
    if isinstance(value, bool):   return "bool"
    if isinstance(value, int):    return "int"
    if isinstance(value, float):  return "float"
    if isinstance(value, str):    return "str"
    return "unknown"


def build_dps_map(probe: dict) -> dict:
    return {
        str(code): {"value": val, "type": infer_type(val)}
        for code, val in probe.get("status_dps", {}).items()
    }


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

def load_registry() -> dict:
    if REGISTRY_FILE.exists():
        with open(REGISTRY_FILE) as f:
            return {d["id"]: d for d in json.load(f).get("devices", [])}
    return {}


def save_registry(devices: dict):
    with open(REGISTRY_FILE, "w") as f:
        json.dump({"devices": list(devices.values())}, f, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(force: bool = False):
    cloud_devices = fetch_cloud_devices()
    ip_map        = scan_lan()

    # attach IPs and normalise version key
    for dev in cloud_devices:
        dev_id = dev.get("id", "")
        dev["ip"]  = ip_map.get(dev_id, dev.get("ip", ""))
        dev["ver"] = str(dev.pop("version", dev.get("ver", "3.3")))

    existing = load_registry()
    updated  = {}

    total = len(cloud_devices)
    print(f"[3/3] Probing {total} device(s) over LAN...\n")
    print(f"  {'Name':<28} {'Type':<8} {'IP':<16} {'DPS codes'}")
    print(f"  {'-'*80}")

    reachable = skipped = 0

    for raw in cloud_devices:
        dev_id   = raw.get("id", "")
        name     = raw.get("name", dev_id)
        ip       = raw.get("ip", "")
        key      = raw.get("key", "")
        ver      = raw.get("ver", "3.3")
        category = raw.get("category", "")
        dev_type = CATEGORY_MAP.get(category.lower(), "unknown")

        # skip re-probe if already has DPS and --force not set
        if not force and dev_id in existing and existing[dev_id].get("dps_map"):
            prev = existing[dev_id]
            prev.update({"ip": ip or prev.get("ip", ""), "key": key, "version": float(ver)})
            updated[dev_id] = prev
            skipped += 1
            print(f"  {name:<28} {dev_type:<8} {ip or '(no IP)':<16} [skipped - already probed]")
            continue

        probe   = probe_device(raw)
        dps_map = build_dps_map(probe) if probe["reachable"] else existing.get(dev_id, {}).get("dps_map", {})

        if probe["reachable"]:
            reachable += 1
            dps_codes = sorted(dps_map.keys(), key=lambda x: int(x))
            ver = str(probe["version_used"])
            print(f"  {name:<28} {dev_type:<8} {ip:<16} {dps_codes}")
        else:
            reason = probe.get("error", "unreachable")
            kept   = " [kept prev DPS]" if dps_map else ""
            print(f"  {name:<28} {dev_type:<8} {ip or '(no IP)':<16} FAIL: {reason}{kept}")

        entry = {
            "id":       dev_id,
            "name":     name,
            "ip":       ip,
            "key":      key,
            "version":  float(ver),
            "category": category,
            "type":     dev_type,
            "dps_map":  dps_map,
            "reachable": probe["reachable"],
        }
        if probe.get("detected_dps"):
            entry["detected_dps_indices"] = probe["detected_dps"]

        # preserve user-edited metadata
        for field in ("friendly_name", "room", "ha_area"):
            if dev_id in existing and field in existing[dev_id]:
                entry[field] = existing[dev_id][field]

        updated[dev_id] = entry

    # keep devices from old registry not returned by cloud (offline, removed)
    for dev_id, dev in existing.items():
        if dev_id not in updated:
            print(f"  {dev.get('name', dev_id):<28} [preserved from previous run - not in cloud]")
            updated[dev_id] = dev

    save_registry(updated)

    print(f"\n  {'-'*80}")
    print(f"  Registry saved -> {REGISTRY_FILE}")
    print(f"  Total: {len(updated)}  |  Reachable: {reachable}  |  Skipped: {skipped}  |  Failed/no IP: {len(updated) - reachable - skipped}")
    print(f"\nNext step:  uv run python bridge.py")


if __name__ == "__main__":
    run(force="--force" in sys.argv)
