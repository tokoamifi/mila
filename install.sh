#!/bin/bash
set -e

echo "🚀 Installer Bot Telegram XL - Siap Pakai"

# Update sistem
sudo apt update && sudo apt upgrade -y

# Install dependensi
sudo apt install -y python3 python3-pip python3-venv git sqlite3 curl

# Clone repo (ganti URL sesuai repo kamu)
REPO_URL="https://github.com/username/nama-repo.git"
FOLDER="bot-xl"

if [ -d "$FOLDER" ]; then
    echo "⚠️ Folder ditemukan. Update dari GitHub..."
    cd "$FOLDER"
    git pull
else
    echo "📥 Clone repo dari GitHub..."
    git clone "$REPO_URL" "$FOLDER"
    cd "$FOLDER"
fi

# Virtual environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install --upgrade pip
pip install -r requirements.txt

# Buat .env jika belum ada
if [ ! -f .env ]; then
    echo "⚙️ Membuat file .env..."
    cat <<EOF > .env
XL_API_KEY=c7d49152-51a3-4cf4-b696-af8bd60ad2d8
HESDA_API_KEY=2iB0Qvvqaw85lCOvCy
HESDA_USERNAME=ikbal192817@gmail.com
HESDA_PASSWORD=ayomasuk123
ADMIN_TELEGRAM_ID=7850206156
ADMIN_TELEGRAM_USERNAME=NIZAM_AKUN
QRIS_STATIS=00020101021226610016ID.CO.SHOPEE.WWW01189360091800221899870208221899870303UMI51440014ID.CO.QRIS.WWW0215ID10254101517530303UMI5204541153033605405125005802ID5909IST STORE6006SERANG61054211162070703A016304428D
EOF
    echo "✅ File .env dibuat. Silakan edit jika perlu."
fi

# Buat folder QRIS jika belum ada
mkdir -p FOTO_QRIS

# Jalankan bot
echo "✅ Instalasi selesai!"
echo "🔧 Untuk menjalankan bot:"
echo "  cd $FOLDER && source venv/bin/activate && python3 botxlx_fixed.py"
