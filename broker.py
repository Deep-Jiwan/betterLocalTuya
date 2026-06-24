"""
Embedded MQTT broker using amqtt (pure Python, no external dependencies).
Runs on localhost:1883 by default.

Can be imported and started as an asyncio task (used by run.py),
or run standalone for testing:
  uv run python broker.py
"""

import asyncio
import logging
import os

from amqtt.broker import Broker
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("broker")


def build_config() -> dict:
    host     = os.getenv("MQTT_HOST",     "localhost")
    port     = int(os.getenv("MQTT_PORT", "47883"))
    username = os.getenv("MQTT_USERNAME", "").strip()
    password = os.getenv("MQTT_PASSWORD", "").strip()

    plugins = {
        "amqtt.plugins.authentication.AnonymousAuthPlugin": {
            "allow_anonymous": not bool(username),
        },
    }

    if username and password:
        passwd_path = "mqtt_passwd"
        with open(passwd_path, "w") as f:
            f.write(f"{username}:{password}\n")
        plugins["amqtt.plugins.authentication.FileAuthPlugin"] = {
            "password-file": passwd_path,
        }

    return {
        "listeners": {
            "default": {
                "type":            "tcp",
                "bind":            f"0.0.0.0:{port}",
                "max_connections": 100,
            },
        },
        "plugins": plugins,
    }


async def start_broker() -> Broker:
    config = build_config()
    host = os.getenv("MQTT_HOST", "localhost")
    port = int(os.getenv("MQTT_PORT", "47883"))

    broker = Broker(config)
    await broker.start()
    log.info("MQTT broker listening on %s:%s", host, port)
    return broker


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    broker = await start_broker()
    try:
        await asyncio.Future()  # run forever
    except (KeyboardInterrupt, asyncio.CancelledError):
        await broker.shutdown()
        log.info("Broker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
