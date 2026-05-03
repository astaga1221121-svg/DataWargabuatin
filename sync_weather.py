import os
import json
import time
import requests
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv

# Load environment variables dari file .env (untuk penggunaan lokal)
load_dotenv()

# Konfigurasi Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "cuaca_realtime" # Sesuai permintaan user

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

def save_to_supabase(data, adm4, village_info):
    """Simpan atau Update data ke Supabase (Upsert) dengan parsing data lengkap"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(f"Supabase credentials not found for {village_info.get('desa')}. Skipping upload.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    try:
        # Parsing data cuaca dari JSON BMKG (mirip logika App.js)
        data_block = data.get("data", [{}])[0]
        root_loc = data.get("lokasi", {})
        inner_loc = data_block.get("lokasi", {})
        loc = {**inner_loc, **root_loc}

        cuaca_blocks = data_block.get("cuaca", [])
        cuaca_now = cuaca_blocks[0][0] if cuaca_blocks and cuaca_blocks[0] else {}
        all_points = [item for sublist in cuaca_blocks for item in sublist] if cuaca_blocks else []

        # Kalkulasi statistik
        temps = [p.get("t") for p in all_points if p.get("t") is not None]
        hus = [p.get("hu") for p in all_points if p.get("hu") is not None]

        avg_t = sum(temps) / len(temps) if temps else None
        avg_h = sum(hus) / len(hus) if hus else None

        payload = {
            "adm4": adm4,
            "adm1": loc.get("adm1"),
            "adm2": loc.get("adm2"),
            "adm3": loc.get("adm3"),
            "provinsi": loc.get("provinsi", village_info.get("provinsi")),
            "kotkab": loc.get("kotkab", village_info.get("kotkab")),
            "kecamatan": loc.get("kecamatan", village_info.get("kecamatan")),
            "desa": loc.get("desa", village_info.get("desa")),
            "lat": float(loc.get("lat", village_info.get("lat", 0))),
            "lon": float(loc.get("lon", village_info.get("lon", 0))),
            "suhu_realtime": cuaca_now.get("t"),
            "kelembapan_realtime": cuaca_now.get("hu"),
            "weather_desc": cuaca_now.get("weather_desc", "Berawan"),
            "weather_icon_url": cuaca_now.get("image") or cuaca_now.get("icon"),
            "rata2_suhu": round(avg_t, 1) if avg_t is not None else None,
            "rata2_hu": round(avg_h, 0) if avg_h is not None else None,
            "suhu_max_hari_ini": max(temps) if temps else None,
            "suhu_min_hari_ini": min(temps) if temps else None,
            "suhu_rata2_hari_ini": round(avg_t, 1) if avg_t is not None else None,
            "forecast_data": data,
            "updated_at": datetime.now().isoformat()
        }

        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
        res = requests.post(url, headers=headers, json=payload)

        if res.status_code not in [200, 201]:
            # Jika error Foreign Key (adm4 tidak ada di tabel lokasi)
            if "violates foreign key constraint" in res.text:
                print(f"Skipping {adm4}: adm4 not found in master 'lokasi' table.")
            else:
                print(f"Supabase Error {res.status_code} for {adm4}: {res.text}")
    except Exception as e:
        print(f"Error uploading to Supabase for {adm4}: {e}")

def cleanup_sampah():
    """Hapus file di sampahku yang lebih dari 1 jam"""
    print("Checking for old files in sampahku...")
    now = datetime.now()
    count = 0
    if os.path.exists(SAMPAH_DIR):
        for f in os.listdir(SAMPAH_DIR):
            file_path = os.path.join(SAMPAH_DIR, f)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if now - file_time > timedelta(hours=1):
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    count += 1
    print(f"Removed {count} expired files from sampahku.")

def process_all_files():
    if not os.path.exists(DATA_DIR):
        print(f"Error: Folder {DATA_DIR} tidak ditemukan.")
        return

    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    excluded = ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json']
    json_files = [f for f in all_files if f not in excluded]

    total_processed = 0

    for json_file in json_files:
        file_path = os.path.join(DATA_DIR, json_file)
        print(f"Processing: {json_file}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
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
                    # Simpan data baru ke cache
                    with open(cache_file, 'w', encoding='utf-8') as cf:
                        json.dump(new_data, cf)

                    # Upload ke Supabase
                    save_to_supabase(new_data, adm4, village)
                    print(f"Updated: {desa} ({adm4})")
                else:
                    print(f"Skip: {desa} (No changes)")

            total_processed += 1
            # Respek rate limit BMKG 60 req/min
            time.sleep(1.1)

    print(f"Finished! Total items processed: {total_processed}")

if __name__ == "__main__":
    cleanup_sampah()
    process_all_files()
