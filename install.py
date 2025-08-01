import os
import subprocess
import platform

REPO = "https://github.com/username/nama-repo.git"
FOLDER = "bot-xl"

def run(cmd):
    print(f"🔧 Menjalankan: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

if os.path.exists(FOLDER):
    print("⚠️ Folder sudah ada. Update dari GitHub...")
    os.chdir(FOLDER)
    run("git pull")
else:
    print("📥 Clone repo dari GitHub...")
    run(f"git clone {REPO} {FOLDER}")
    os.chdir(FOLDER)

print("📦 Install dependencies...")
run("python -m pip install --upgrade pip")
run("pip install -r requirements.txt")

if not os.path.exists(".env"):
    print("⚙️ Membuat file .env...")
    with open(".env", "w") as f:
        f.write("XL_API_KEY=c7d49152-51a3-4cf4-b696-af8bd60ad2d8\n")
        f.write("HESDA_API_KEY=2iB0Qvvqaw85lCOvCy\n")
        f.write("HESDA_USERNAME=ikbal192817@gmail.com\n")
        f.write("HESDA_PASSWORD=ayomasuk123\n")
        f.write("ADMIN_TELEGRAM_ID=7850206156\n")
        f.write("ADMIN_TELEGRAM_USERNAME=NIZAM_AKUN\n")
        f.write("QRIS_STATIS=00020101021226610016ID.CO.SHOPEE.WWW01189360091800221899870208221899870303UMI51440014ID.CO.QRIS.WWW0215ID10254101517530303UMI5204541153033605405125005802ID5909IST STORE6006SERANG61054211162070703A016304428D\n")
    print("✅ File .env dibuat. Silakan edit jika perlu.")

os.makedirs("FOTO_QRIS", exist_ok=True)

print("✅ Instalasi selesai!")
print("🔧 Untuk menjalankan bot:")
print("  cd bot-xl && python botxlx_fixed.py")
