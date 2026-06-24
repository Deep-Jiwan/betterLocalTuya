# betterLocalTuya

> Local-first Tuya → MQTT → Home Assistant bridge with sub-100ms command latency, zero cloud dependency at runtime, and a built-in web UI.

---

## What makes this different

**Sub-100ms command latency** via `select()` + socketpair wakeup. The device thread blocks on `select()` watching the Tuya socket and a local pipe. Sending a command writes one byte to the pipe, unblocking `select()` within microseconds — no polling interval.

**Persistent connections with automatic recovery.** One long-lived TCP socket per device. TCP keepalive probes (60s idle, 10s interval, 3 probes) keep router NAT entries alive and detect dead connections before a command needs to be sent. Exponential backoff on reconnect so offline devices don't hammer the network.

**Correct multi-switch state.** Tuya devices return partial DPS maps (e.g. only `{"2": true}` when you toggle switch 2). A merge cache accumulates all known DPS values before each publish, preventing other switches from appearing to turn off in Home Assistant.

**Flood control.** A coalescing command map (`dict[dps → value]`) under a lock absorbs rapid-fire commands — a burst of 1000 identical commands collapses to a single send. 150ms pacing protects device firmware.

**Auto-discovery.** One scan via the Tuya Cloud API fetches all device IPs and local encryption keys. After that, no cloud calls happen at runtime — everything is local.

**Zero external dependencies.** An embedded MQTT broker (amqtt) runs in-process. No Mosquitto, no external broker service needed.

**Built-in web UI.** Monitor device state, send commands, re-run discovery, and edit settings — all without touching config files or restarting the container.

---

## How it works

```
Tuya Device (LAN, port 6668)
        │  persistent TCP
        ▼
   DeviceWorker
   ├─ select() loop + socketpair wakeup
   ├─ coalescing command map
   ├─ TCP keepalive
   └─ exponential backoff
        │
        ▼ DPS state (merge cache)
        │
   MQTT Broker (amqtt, embedded :47883)
        │
        ▼
   Home Assistant  ←── HA MQTT Discovery (auto-registered entities)
```

---

## Getting started

### Prerequisites
- Docker + Docker Compose
- Tuya developer account ([iot.tuya.com](https://iot.tuya.com)) — for the one-time device discovery only
- Home Assistant with MQTT integration

### 1. Clone

```bash
git clone https://github.com/Deep-Jiwan/betterLocalTuya.git
cd betterLocalTuya
mkdir -p data
```

### 2. Configure

Edit `docker-compose.yml` and fill in your Tuya credentials:

```yaml
environment:
  TUYA_CLIENT_ID: your_client_id
  TUYA_SECRET:    your_secret
  TUYA_REGION:    eu          # eu | us | us-e | cn | in
```

Or pull the published image directly — no build needed:

```yaml
services:
  betterlocaltuya:
    image: ghcr.io/deep-jiwan/betterlocaltuya:latest
    container_name: betterlocaltuya
    restart: unless-stopped
    ports:
      - "47883:47883"   # MQTT broker
      - "47090:47090"   # Web UI
      - "47765:47765"   # Health
    environment:
      TUYA_CLIENT_ID: your_client_id
      TUYA_SECRET:    your_secret
      TUYA_REGION:    eu
      MQTT_HOST:      localhost
      MQTT_PORT:      "47883"
      MQTT_USERNAME:  ""
      MQTT_PASSWORD:  ""
      WEB_PORT:       "47090"
      HEALTH_PORT:    "47765"
    volumes:
      - ./data/devices_registry.json:/app/devices_registry.json
      - ./data/.env:/app/.env
      - ./data/logs:/app/logs
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:47765/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 40s
```

### 3. Run discovery

```bash
docker compose up -d
```

Open `http://localhost:47090`, go to the **Discovery** tab, and click **Run Discovery**. This calls the Tuya Cloud API once to fetch device IPs and local keys, saves them to `data/devices_registry.json`, then runs entirely locally from that point on.

### 4. Connect Home Assistant

Settings → Integrations → MQTT:
- **Broker**: your host machine's LAN IP (e.g. `192.168.1.100`) — not `localhost`
- **Port**: `47883`
- Username / Password: leave blank unless you configured them

Entities appear automatically under the MQTT integration — no manual entity configuration.

> **Windows / Docker Desktop:** add a firewall rule so HA can reach the broker:
> ```powershell
> # Run as Administrator
> New-NetFirewallRule -DisplayName "betterLocalTuya MQTT" -Direction Inbound -Protocol TCP -LocalPort 47883 -Action Allow -Profile Any
> ```

---

## Web UI

`http://localhost:47090`

| Tab | What's there |
|---|---|
| Monitoring | Live device status, online/offline indicators, uptime, scrolling log stream |
| Control | Per-device DPS toggles and values, instant send |
| Discovery | Re-run device scan, stream output live |
| Settings | Edit credentials and MQTT config without restarting |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `TUYA_CLIENT_ID` | — | Tuya Cloud app client ID |
| `TUYA_SECRET` | — | Tuya Cloud app secret |
| `TUYA_REGION` | `eu` | Cloud API region (`eu` / `us` / `us-e` / `cn` / `in`) |
| `MQTT_HOST` | `localhost` | MQTT broker host |
| `MQTT_PORT` | `47883` | MQTT broker port |
| `MQTT_USERNAME` | `` | Optional broker auth |
| `MQTT_PASSWORD` | `` | Optional broker auth |
| `WEB_PORT` | `47090` | Web UI port |
| `HEALTH_PORT` | `47765` | Health endpoint port |

---

## Persistent data

Mount `./data/` into the container to survive restarts:

```
data/
  devices_registry.json   # local keys + IPs from discovery (keep private, do not commit)
  .env                    # credential overrides (keep private, do not commit)
  logs/                   # rotating logs — 5 MB × 5 files
```

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

Used by Docker's built-in healthcheck — `docker ps` shows `(healthy)` once all services are up.

---

## Built on

- **[tinytuya](https://github.com/jasonacox/tinytuya)** — pure-Python Tuya LAN protocol implementation. Handles protocol versions 3.1 / 3.3 / 3.4 / 3.5, encryption, heartbeat, and the raw socket interface this bridge drives directly.
- **[amqtt](https://github.com/Yakifo/amqtt)** — pure-Python async MQTT broker. Runs embedded in the same process; no external broker binary or service needed.

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=Deep-Jiwan/betterLocalTuya&type=Date)](https://star-history.com/#Deep-Jiwan/betterLocalTuya&Date)
