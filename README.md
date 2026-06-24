# betterLocalTuya

> Local-first Tuya → MQTT → Home Assistant bridge with sub-100ms latency, zero cloud dependency at runtime, and a built-in web UI.

[![Star History Chart](https://api.star-history.com/svg?repos=Deep-Jiwan/betterLocalTuya&type=Date)](https://star-history.com/#Deep-Jiwan/betterLocalTuya&Date)

---

## Why not just LocalTuya?

[LocalTuya](https://github.com/rospogrigio/localtuya) is a HA custom component — it lives inside Home Assistant, one integration instance per device, each polling independently. This project takes a different approach:

| | LocalTuya | betterLocalTuya |
|---|---|---|
| Connection model | One HA entity = one TCP socket | Persistent per-device socket, shared MQTT bus |
| Command latency | 300ms–2s (poll cycle + reconnect) | <100ms (select+socketpair wakeup) |
| Idle reconnect | Silent stale socket → slow first command | TCP keepalive probes prevent router conntrack timeout |
| Multi-switch state | Can corrupt (partial DPS overwrite) | Merge cache: partial DPS updates are accumulated before publish |
| Burst commands | Each sent immediately, firmware overwhelmed | Coalescing map collapses burst to one send per DPS key |
| Device discovery | Manual IP + key entry per device | Auto-discovery via Tuya Cloud API, one-click re-scan |
| Deployment | HA restart required for config changes | Standalone Docker container, HA just needs MQTT |
| Observability | HA logs only | Built-in web UI with live log stream, per-device stats, control panel |

---

## How it works

```
Tuya Device (LAN, port 6668)
        │  persistent TCP  │  select() loop
        ▼                  │
   DeviceWorker ──────────►│  socketpair wakeup
        │                  │  coalescing command map
        │  DPS state       │  TCP keepalive (60s idle probe)
        ▼                  │
   State Cache (merge)     │  exponential backoff on failure
        │
        ▼
   MQTT Broker (amqtt, embedded, port 47883)
        │
        ▼
   Home Assistant  ←──────  HA MQTT Discovery (auto-registered entities)
```

**Key design decisions:**

- **`select()` + socketpair wakeup** — the device thread blocks on `select()` watching the Tuya socket and a local pipe. `send_command()` writes one byte to the pipe, unblocking `select()` within microseconds. No polling interval.
- **Coalescing command map** — `dict[dps → value]` under a lock. A flood of 1000 identical commands collapses to a single send. 150ms pacing prevents firmware overload.
- **State merge cache** — Tuya devices return partial DPS maps (e.g. only `{"2": true}`). Without merging, publishing this as the full state clobbers other switches in HA. The cache accumulates all known DPS before each publish.
- **TCP keepalive** — `SO_KEEPALIVE` with 60s idle, 10s interval, 3 probes. Keeps router NAT/conntrack entries alive and detects dead connections proactively, avoiding the 2–3s OS timeout on first command after idle.
- **Embedded MQTT broker** — no external Mosquitto needed. amqtt runs in-process; the bridge connects to itself.
- **HA MQTT Discovery** — entities register themselves on startup via `homeassistant/<type>/<uid>/config` retained topics. Zero HA configuration required beyond pointing it at the broker.

---

## Getting started

### Prerequisites
- Docker + Docker Compose
- Tuya developer account ([iot.tuya.com](https://iot.tuya.com)) — for the one-time device discovery only
- Home Assistant with MQTT integration

### 1. Clone and configure

```bash
git clone https://github.com/Deep-Jiwan/betterLocalTuya.git
cd betterLocalTuya
mkdir -p data
```

Edit `docker-compose.yml` and fill in your Tuya credentials:

```yaml
environment:
  TUYA_CLIENT_ID: your_client_id
  TUYA_SECRET:    your_secret
  TUYA_REGION:    eu          # eu | us | us-e | cn | in
```

### 2. Run discovery

Open the web UI at `http://localhost:47090`, go to the **Discovery** tab, and click **Run Discovery**. This calls the Tuya Cloud API once to fetch device IPs and local keys, then saves them locally. No cloud calls happen after this.

### 3. Start the bridge

```bash
docker compose up -d
```

Check it's healthy:
```bash
docker exec tuyamqtt curl -sf http://localhost:47765/health
```

### 4. Connect Home Assistant

In HA → Settings → Integrations → MQTT:
- **Broker**: your host machine's LAN IP (e.g. `192.168.1.100`)
- **Port**: `47883`
- Username/Password: leave blank unless configured

Devices appear automatically under HA's MQTT integration — no entity configuration needed.

> **Windows/Docker Desktop note:** add a Windows Firewall inbound rule for TCP port 47883 so HA can reach the broker:
> ```powershell
> # Run as Administrator
> New-NetFirewallRule -DisplayName "betterLocalTuya MQTT" -Direction Inbound -Protocol TCP -LocalPort 47883 -Action Allow -Profile Any
> ```

---

## Web UI

`http://localhost:47090`

| Tab | What's there |
|---|---|
| Monitoring | Live device status, online/offline indicators, uptime, log stream |
| Control | Per-device DPS toggles and values, instant send |
| Discovery | Re-run device scan, stream output live |
| Settings | Edit `.env` (credentials, region, MQTT config) without restarting |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `TUYA_CLIENT_ID` | — | Tuya Cloud app client ID |
| `TUYA_SECRET` | — | Tuya Cloud app secret |
| `TUYA_REGION` | `eu` | Cloud API region |
| `MQTT_HOST` | `localhost` | MQTT broker host |
| `MQTT_PORT` | `47883` | MQTT broker port |
| `MQTT_USERNAME` | `` | Optional broker auth |
| `MQTT_PASSWORD` | `` | Optional broker auth |
| `WEB_PORT` | `47090` | Web UI port |
| `HEALTH_PORT` | `47765` | Health endpoint port |

---

## Persistent data

The `./data/` directory is mounted into the container:

```
data/
  devices_registry.json   # local keys + IPs from discovery (never commit this)
  .env                    # credential overrides (never commit this)
  logs/                   # rotating logs, 5 MB × 5 files
```

---

## Built on

- **[tinytuya](https://github.com/jasonacox/tinytuya)** — pure-Python Tuya LAN protocol implementation. Handles protocol versions 3.1 / 3.3 / 3.4 / 3.5, encryption, heartbeat, and the raw socket interface this bridge drives directly.
- **[amqtt](https://github.com/Yakifo/amqtt)** — pure-Python async MQTT broker. Runs embedded in the same process; no external broker binary or service required.

---

## Health check

```
GET http://localhost:47765/health
```

```json
{
  "status": "ok",
  "uptime_s": 3600,
  "devices_total": 14,
  "devices_online": 14,
  "devices_offline": 0,
  "devices": [...]
}
```

Used by Docker's built-in healthcheck (`docker ps` shows `(healthy)`).
