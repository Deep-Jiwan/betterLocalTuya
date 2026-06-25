#!/usr/bin/env bash
# install.sh — betterLocalTuya full setup on a fresh Linux machine
# Usage: curl -fsSL https://raw.githubusercontent.com/Deep-Jiwan/betterLocalTuya/main/install.sh | bash
set -euo pipefail

REPO_URL="https://github.com/Deep-Jiwan/betterLocalTuya.git"
INSTALL_DIR="${INSTALL_DIR:-$HOME/betterLocalTuya}"
SERVICE_NAME="betterlocaltuya"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[*]${NC} $*"; }
success() { echo -e "${GREEN}[✓]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
die()     { echo -e "${RED}[✗]${NC} $*"; exit 1; }

echo ""
echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
echo -e "${CYAN}║       betterLocalTuya Installer      ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
echo ""

# ── Root check ────────────────────────────────────────────────────────────────
# If running as root, commands that need sudo just run directly
if [[ $EUID -eq 0 ]]; then
    warn "Running as root."
    shopt -s expand_aliases
    alias sudo=""
fi

# ── System packages ───────────────────────────────────────────────────────────
info "Updating package lists..."
sudo apt-get update -qq

info "Upgrading installed packages..."
sudo apt-get upgrade -y -qq

info "Installing dependencies (curl, git)..."
sudo apt-get install -y -qq curl git

success "System packages ready."

# ── uv ────────────────────────────────────────────────────────────────────────
if command -v uv &>/dev/null; then
    success "uv already installed ($(uv --version))"
else
    info "Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # add to current session PATH
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    success "uv installed ($(uv --version))"
fi

# make sure uv is on PATH for the rest of the script
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

# ── Clone repo ────────────────────────────────────────────────────────────────
if [[ -d "$INSTALL_DIR/.git" ]]; then
    info "Repo already cloned at $INSTALL_DIR — pulling latest..."
    git -C "$INSTALL_DIR" pull --ff-only
else
    info "Cloning betterLocalTuya into $INSTALL_DIR..."
    git clone "$REPO_URL" "$INSTALL_DIR"
fi

cd "$INSTALL_DIR"
success "Repo ready at $INSTALL_DIR"

# ── Python deps ───────────────────────────────────────────────────────────────
info "Installing Python dependencies..."
uv sync --frozen
success "Python environment ready."

# ── data/ directory ───────────────────────────────────────────────────────────
mkdir -p data/logs

# ── Credentials ───────────────────────────────────────────────────────────────
ENV_FILE="data/.env"

if [[ -f "$ENV_FILE" ]] && grep -q "TUYA_CLIENT_ID" "$ENV_FILE" 2>/dev/null; then
    warn "Existing $ENV_FILE found — skipping credential prompt."
    warn "Edit $INSTALL_DIR/$ENV_FILE to change credentials."
else
    echo ""
    echo -e "${CYAN}── Tuya Cloud credentials ──────────────────────────────────────────${NC}"
    echo "  Get these from https://iot.tuya.com → Cloud → your project → Overview"
    echo ""

    read -rp "  TUYA_CLIENT_ID  : " CLIENT_ID
    read -rsp "  TUYA_SECRET     : " SECRET
    echo ""

    echo "  TUYA_REGION options:"
    echo "    eu    Europe / Frankfurt"
    echo "    us    Western America"
    echo "    us-e  Eastern America / Virginia"
    echo "    cn    China"
    echo "    in    India"
    read -rp "  TUYA_REGION [eu]: " REGION
    REGION="${REGION:-eu}"

    cat > "$ENV_FILE" <<EOF
TUYA_CLIENT_ID=${CLIENT_ID}
TUYA_SECRET=${SECRET}
TUYA_REGION=${REGION}
MQTT_HOST=localhost
MQTT_PORT=47883
MQTT_USERNAME=
MQTT_PASSWORD=
WEB_PORT=47090
HEALTH_PORT=47765
EOF

    success "Credentials saved to $ENV_FILE"
fi

# load env for the current session
set -o allexport
source "$ENV_FILE"
set +o allexport

# ── Firewall ──────────────────────────────────────────────────────────────────
if command -v ufw &>/dev/null && sudo ufw status | grep -q "Status: active"; then
    info "Opening firewall ports (MQTT 47883, Web UI 47090)..."
    sudo ufw allow 47883/tcp comment "betterLocalTuya MQTT" >/dev/null
    sudo ufw allow 47090/tcp comment "betterLocalTuya Web UI" >/dev/null
    success "Firewall rules added."
fi

# ── Discovery ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}── Device discovery ────────────────────────────────────────────────${NC}"
echo "  This will fetch devices from Tuya Cloud and scan your LAN."
echo "  Run this from a machine on the SAME network/VLAN as your Tuya devices."
echo ""
read -rp "  Run discovery now? [Y/n]: " RUN_DISC
RUN_DISC="${RUN_DISC:-Y}"

if [[ "$RUN_DISC" =~ ^[Yy]$ ]]; then
    info "Running discovery..."
    uv run python discover.py
    success "Discovery complete."
else
    warn "Skipping discovery. Run manually: cd $INSTALL_DIR && uv run python discover.py"
fi

# ── systemd service ───────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}── systemd service ─────────────────────────────────────────────────${NC}"
read -rp "  Install as a systemd service (auto-start on boot)? [Y/n]: " INSTALL_SVC
INSTALL_SVC="${INSTALL_SVC:-Y}"

if [[ "$INSTALL_SVC" =~ ^[Yy]$ ]]; then
    UV_BIN="$(command -v uv)"
    SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

    sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=betterLocalTuya - Tuya to MQTT bridge
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${UV_BIN} run python run.py
Restart=always
RestartSec=10
EnvironmentFile=${INSTALL_DIR}/${ENV_FILE}
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE_NAME"
    sudo systemctl start "$SERVICE_NAME"

    sleep 3
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        success "Service started and enabled on boot."
    else
        warn "Service installed but failed to start. Check: journalctl -u $SERVICE_NAME -n 50"
    fi
else
    echo ""
    info "To start manually:"
    echo "    cd $INSTALL_DIR && uv run python run.py"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           Setup complete!            ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════╝${NC}"
echo ""
echo "  Web UI   →  http://$(hostname -I | awk '{print $1}'):47090"
echo "  MQTT     →  $(hostname -I | awk '{print $1}'):47883"
echo "  Health   →  curl http://localhost:47765/health"
echo ""
if [[ "$INSTALL_SVC" =~ ^[Yy]$ ]]; then
echo "  Service control:"
echo "    sudo systemctl status $SERVICE_NAME"
echo "    sudo systemctl restart $SERVICE_NAME"
echo "    journalctl -u $SERVICE_NAME -f"
fi
echo ""
