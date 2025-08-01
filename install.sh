#!/bin/bash

# ===================================================================
# Installer Final Bot XL (Perbaikan Interaktif API Key)
# ===================================================================

# Warna
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Variabel Proyek
REPO_URL="https://github.com/tokoamifi/mila.git"
INSTALL_DIR="/opt/bot_ppob"
SERVICE_NAME="bot-ppob.service"

# Fungsi pengecekan root
check_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        echo -e "${RED}Error: Skrip ini harus dijalankan sebagai root.${NC}"
        exit 1
    fi
}

# Fungsi Membersihkan instalasi lama
cleanup_previous_installation() {
    echo -e "\n${YELLOW}--> Membersihkan instalasi sebelumnya...${NC}"
    if systemctl is-active --quiet $SERVICE_NAME; then
        systemctl stop $SERVICE_NAME
    fi
    if systemctl is-enabled --quiet $SERVICE_NAME; then
        systemctl disable $SERVICE_NAME
    fi
    rm -f /etc/systemd/system/$SERVICE_NAME
    systemctl daemon-reload
    if [ -d "$INSTALL_DIR" ]; then
        rm -rf $INSTALL_DIR
    fi
    echo -e "${GREEN}--> Pembersihan selesai.${NC}"
}

# --- FUNGSI INPUT YANG DIPERBARUI ---
get_user_input() {
    echo -e "${YELLOW}===== Konfigurasi Bot & API =====${NC}"
    read -p "Masukkan BOT_TOKEN Anda: " BOT_TOKEN
    read -p "Masukkan ADMIN_TELEGRAM_ID Anda: " ADMIN_TELEGRAM_ID
    read -p "Masukkan KMSP_API_KEY (XL_API_KEY) Anda: " KMSP_API_KEY
}

# Fungsi instalasi dependensi
install_dependencies() {
    echo -e "\n${GREEN}--> Menginstal dependensi...${NC}"
    apt-get update > /dev/null 2>&1
    apt-get install -y python3-pip python3-venv git curl > /dev/null 2>&1
}

# Fungsi untuk clone repo dan konfigurasi
setup_project() {
    echo -e "\n${GREEN}--> Mengkloning repositori dari GitHub...${NC}"
    git clone $REPO_URL $INSTALL_DIR
    
    echo -e "\n${GREEN}--> Menginstal library Python...${NC}"
    python3 -m venv "$INSTALL_DIR/venv"
    "$INSTALL_DIR/venv/bin/pip" install "python-telegram-bot[job-queue]" requests > /dev/null 2>&1
}

# --- SKRIP RUN YANG DIPERBARUI ---
create_run_script() {
    echo -e "\n${GREEN}--> Membuat skrip 'run.sh' dengan konfigurasi Anda...${NC}"
    
    # Kredensial lain tetap hardcoded sesuai skrip asli
    HESDA_API_KEY="2iB0Qvvqaw85lCOvCy"
    HESDA_USERNAME="hokagelegend9999@gmail.com"
    HESDA_PASSWORD="ayomasuk123"
    ADMIN_USERNAME_FALLBACK="hokagelegend1"
    QRIS_STATIS="00020101021226610016ID.CO.SHOPEE.WWW01189360091800221899870208221899870303UMI51440014ID.CO.QRIS.WWW0215ID10254101517530303UMI5204541153033605405125005802ID5909IST STORE6006SERANG61054211162070703A016304428D"

    cat << EOF > "$INSTALL_DIR/run.sh"
#!/bin/bash

# Mengaktifkan virtual environment
source "$INSTALL_DIR/venv/bin/activate"

# Mengatur environment variables untuk bot
export BOT_TOKEN="$BOT_TOKEN"
export ADMIN_TELEGRAM_ID="$ADMIN_TELEGRAM_ID"
export XL_API_KEY="$KMSP_API_KEY" # Menggunakan API Key dari input Anda
export HESDA_API_KEY="$HESDA_API_KEY"
export HESDA_USERNAME="$HESDA_USERNAME"
export HESDA_PASSWORD="$HESDA_PASSWORD"
export ADMIN_TELEGRAM_USERNAME="$ADMIN_USERNAME_FALLBACK"
export QRIS_STATIS="$QRIS_STATIS"

# Menjalankan skrip bot utama
echo "Menjalankan bot dari run.sh..."
python3 "$INSTALL_DIR/botxlx_fixed.py"
EOF

    chmod +x "$INSTALL_DIR/run.sh"
}

# Fungsi membuat layanan systemd
create_systemd_service() {
    echo -e "\n${GREEN}--> Membuat dan menjalankan layanan systemd ($SERVICE_NAME)...${NC}"
    cat << EOF > /etc/systemd/system/$SERVICE_NAME
[Unit]
Description=Bot Telegram PPOB (Final)
After=network.target

[Service]
User=root
Group=root
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/run.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable $SERVICE_NAME
    systemctl start $SERVICE_NAME
}

# --- Fungsi Utama ---
main() {
    check_root
    cleanup_previous_installation
    get_user_input
    
    echo -e "\n${YELLOW}Memulai instalasi Bot PPOB...${NC}"
    install_dependencies
    setup_project
    create_run_script
    create_systemd_service
    
    echo -e "\n\n${GREEN}=====================================================${NC}"
    echo -e "${GREEN}      ✅ Instalasi Bot Selesai!${NC}"
    echo -e "${GREEN}=====================================================${NC}"
    echo -e "Layanan bot sekarang ${GREEN}sudah berjalan${NC} dengan nama ${YELLOW}$SERVICE_NAME${NC}"
    echo -e "\nUntuk mengecek status: ${YELLOW}systemctl status $SERVICE_NAME${NC}"
    echo -e "Untuk melihat log: ${YELLOW}journalctl -u $SERVICE_NAME -f${NC}"
    echo ""
}

main
