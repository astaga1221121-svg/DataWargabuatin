import os
import json
import time
import requests
from datetime import datetime, timedelta
import shutil

# Konfigurasi Supabase
# Ambil dari Environment Variables GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "weather_forecasts" # Sesuaikan dengan nama tabel Anda

DATA_DIR = "data"
CACHE_DIR = "cache"
SAMPAH_DIR = "sampahku"

# Pastikan folder ada
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(SAMPAH_DIR, exist_ok=True)

def fetch_weather(url):
    """Fetch data dari API BMKG dengan retry logic"""
    for _ in range(3):
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print("Rate limit hit, waiting longer...")
                time.sleep(10)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(2)
    return None

def save_to_supabase(data, adm4, village_name):
    """Simpan atau Update data ke Supabase (Upsert)"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials not found. Skipping upload.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    # Payload disesuaikan agar frontend mudah mengkonsumsi
    payload = {
        "adm4": adm4,
        "desa": village_name,
        "forecast_data": data,
        "last_update": datetime.now().isoformat()
    }

    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        # Menggunakan POST dengan ON CONFLICT (adm4) di Supabase
        res = requests.post(url, headers=headers, json=payload)
        if res.status_code not in [200, 201]:
            print(f"Supabase Error {res.status_code}: {res.text}")
    except Exception as e:
        print(f"Error uploading to Supabase for {adm4}: {e}")

def cleanup_sampah():
    """Hapus file di sampahku yang lebih dari 1 jam"""
    print("Checking for old files in sampahku...")
    now = datetime.now()
    count = 0
    for f in os.listdir(SAMPAH_DIR):
        file_path = os.path.join(SAMPAH_DIR, f)
        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if now - file_time > timedelta(hours=1):
            if os.path.isfile(file_path):
                os.remove(file_path)
                count += 1
    print(f"Removed {count} expired files from sampahku.")

def process_all_files():
    # Ambil semua file .json di folder data
    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    # Filter file yang bukan data desa
    excluded = ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json']
    json_files = [f for f in all_files if f not in excluded]

    total_processed = 0

    for json_file in json_files:
        file_path = os.path.join(DATA_DIR, json_file)
        print(f"Processing: {json_file}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                # Penanganan jika file kosong atau format tidak valid
                if not content: continue
                villages = json.loads(content)
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
            continue

        for village in villages:
            adm4 = village.get('adm4')
            url = village.get('url')
            desa = village.get('desa', '')

            if not adm4 or not url: continue

            cache_file = os.path.join(CACHE_DIR, f"{adm4}.json")
            new_data = fetch_weather(url)

            if new_data:
                # Cek update: bandingkan data baru dengan cache
                should_update = True
                if os.path.exists(cache_file):
                    try:
                        with open(cache_file, 'r', encoding='utf-8') as cf:
                            old_data = json.load(cf)
                        if old_data == new_data:
                            should_update = False
                        else:
                            # Pindahkan yang lama ke sampahku
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            shutil.copy2(cache_file, os.path.join(SAMPAH_DIR, f"{adm4}_{timestamp}.json"))
                    except:
                        pass

                if should_update:
                    # Update cache
                    with open(cache_file, 'w', encoding='utf-8') as cf:
                        json.dump(new_data, cf)

                    # Update Supabase
                    save_to_supabase(new_data, adm4, desa)
                    print(f"Updated: {desa} ({adm4})")
                else:
                    print(f"Skip: {desa} (No changes)")

            total_processed += 1
            # Rate limit: max 60 requests per minute (1 per second)
            time.sleep(1.1)

    print(f"Finished! Total items processed: {total_processed}")

if __name__ == "__main__":
    cleanup_sampah()
    process_all_files()
