"""
One-time setup script: checks prerequisites and installs all dependencies.

Usage:
  python install.py
"""

import os
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"  > {' '.join(cmd)}")
    return subprocess.run(cmd, check=check)


def check_python_version():
    major, minor = sys.version_info[:2]
    if (major, minor) < (3, 11):
        print(f"ERROR: Python 3.11+ required, found {major}.{minor}")
        sys.exit(1)
    print(f"[OK] Python {major}.{minor}")


def check_uv() -> bool:
    result = subprocess.run(["uv", "--version"], capture_output=True)
    if result.returncode == 0:
        print(f"[OK] uv found: {result.stdout.decode().strip()}")
        return True
    return False


def install_uv():
    print("[..] uv not found — installing...")
    if sys.platform == "win32":
        run(["powershell", "-ExecutionPolicy", "ByPass", "-c",
             "irm https://astral.sh/uv/install.ps1 | iex"])
    else:
        run(["sh", "-c", "curl -LsSf https://astral.sh/uv/install.sh | sh"])
    print("[OK] uv installed — you may need to restart your terminal if PATH wasn't updated")


def install_dependencies():
    print("[..] Installing Python dependencies via uv...")
    run(["uv", "sync"])
    print("[OK] Dependencies installed")


def check_env():
    env_file = Path(".env")
    if not env_file.exists():
        print("[!!] .env file not found — creating template...")
        env_file.write_text(
            "TUYA_CLIENT_ID=\n"
            "TUYA_SECRET=\n"
            "TUYA_REGION=eu\n"
            "\n"
            "MQTT_HOST=localhost\n"
            "MQTT_PORT=1883\n"
            "MQTT_USERNAME=\n"
            "MQTT_PASSWORD=\n"
        )
        print("[!!] Fill in your Tuya credentials in .env before continuing")
        return False

    from dotenv import load_dotenv
    load_dotenv()
    missing = [k for k in ("TUYA_CLIENT_ID", "TUYA_SECRET", "TUYA_REGION")
               if not os.getenv(k, "").strip()]
    if missing:
        print(f"[!!] Missing in .env: {', '.join(missing)}")
        return False

    print("[OK] .env looks good")
    return True


def main():
    print("=" * 50)
    print(" TuyaMQTT Setup")
    print("=" * 50)
    print()

    # 1. Python version
    check_python_version()

    # 2. uv
    if not check_uv():
        install_uv()
        if not check_uv():
            print("ERROR: uv still not found after install. Add it to PATH and re-run.")
            sys.exit(1)

    # 3. Dependencies
    install_dependencies()

    # 4. .env
    env_ok = check_env()

    print()
    print("=" * 50)
    if env_ok:
        print(" Setup complete!")
        print()
        print(" Next steps:")
        print("   1. Run discovery:   uv run python discover.py")
        print("   2. Start the stack: uv run python run.py")
    else:
        print(" Almost done — fill in .env then run:")
        print("   python install.py   (to verify)")
        print("   uv run python discover.py")
    print("=" * 50)


if __name__ == "__main__":
    main()
