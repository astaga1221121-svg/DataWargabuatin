import os
import json
import time
import requests
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Konfigurasi Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

DATA_DIR = "data"
CACHE_DIR = "cache"
SAMPAH_DIR = "sampahku"

# Pastikan folder ada
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(SAMPAH_DIR, exist_ok=True)

# Gunakan Session untuk efisiensi koneksi (lebih cepat)
session = requests.Session()

def fetch_weather(url):
    """Fetch data dari API BMKG dengan retry logic"""
    for _ in range(3):
        try:
            response = session.get(url, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print("Rate limit hit BMKG, waiting 10s...")
                time.sleep(10)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            time.sleep(2)
    return None

def save_to_supabase(data, adm4, village_info):
    """Otomatis isi tabel 'lokasi' lalu 'cuaca_realtime' (Upsert)"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    try:
        # 1. Parsing data dari JSON BMKG
        data_block = data.get("data", [{}])[0]
        root_loc = data.get("lokasi", {})
        inner_loc = data_block.get("lokasi", {})
        loc = {**inner_loc, **root_loc}

        # 2. OTOMATIS ISI TABEL LOKASI (Master Data)
        # Menjamin Foreign Key adm4 ada sebelum data cuaca masuk
        lokasi_payload = {
            "adm4": adm4,
            "desa": loc.get("desa", village_info.get("desa")),
            "kecamatan": loc.get("kecamatan", village_info.get("kecamatan")),
            "kotkab": loc.get("kotkab", village_info.get("kotkab")),
            "provinsi": loc.get("provinsi", village_info.get("provinsi")),
            "lat": float(loc.get("lat", village_info.get("lat", 0))),
            "lon": float(loc.get("lon", village_info.get("lon", 0))),
            "url_bmkg": village_info.get("url")
        }

        session.post(f"{SUPABASE_URL}/rest/v1/lokasi", headers=headers, json=lokasi_payload)

        # 3. Parsing Data Cuaca Realtime & Statistik
        cuaca_blocks = data_block.get("cuaca", [])
        cuaca_now = cuaca_blocks[0][0] if cuaca_blocks and cuaca_blocks[0] else {}
        all_points = [item for sublist in cuaca_blocks for item in sublist] if cuaca_blocks else []

        temps = [p.get("t") for p in all_points if p.get("t") is not None]
        hus = [p.get("hu") for p in all_points if p.get("hu") is not None]

        avg_t = sum(temps) / len(temps) if temps else None
        avg_h = sum(hus) / len(hus) if hus else None

        # 4. OTOMATIS ISI TABEL CUACA_REALTIME
        cuaca_payload = {
            "adm4": adm4,
            "adm1": loc.get("adm1"),
            "adm2": loc.get("adm2"),
            "adm3": loc.get("adm3"),
            "provinsi": lokasi_payload["provinsi"],
            "kotkab": lokasi_payload["kotkab"],
            "kecamatan": lokasi_payload["kecamatan"],
            "desa": lokasi_payload["desa"],
            "lat": lokasi_payload["lat"],
            "lon": lokasi_payload["lon"],
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

        res = session.post(f"{SUPABASE_URL}/rest/v1/cuaca_realtime", headers=headers, json=cuaca_payload)
        if res.status_code not in [200, 201]:
            print(f"Supabase Error for {adm4}: {res.text}")

    except Exception as e:
        print(f"Error processing {adm4}: {e}")

def process_all_files():
    if not os.path.exists(DATA_DIR): return

    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    excluded = ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json', 'fix_json.py']
    json_files = sorted([f for f in all_files if f not in excluded])

    total = 0
    for json_file in json_files:
        print(f"\n>>> File: {json_file}")
        try:
            with open(os.path.join(DATA_DIR, json_file), 'r', encoding='utf-8') as f:
                villages = json.load(f)
        except: continue

        for village in villages:
            adm4 = village.get('adm4')
            url = village.get('url')
            if not adm4 or not url: continue

            cache_file = os.path.join(CACHE_DIR, f"{adm4}.json")

            # 1. Fetch data baru
            new_data = fetch_weather(url)
            if not new_data: continue

            # 2. Cek apakah ada perubahan dibanding cache
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as cf:
                        if json.load(cf) == new_data:
                            print(f"Skip {adm4} (Tetap)")
                            time.sleep(1.05) # Delay tetap agar tidak kena ban
                            continue
                except: pass

            # 3. Simpan Cache & Push ke Supabase
            with open(cache_file, 'w', encoding='utf-8') as cf:
                json.dump(new_data, cf)

            save_to_supabase(new_data, adm4, village)
            print(f"Sync: {village.get('desa', adm4)}")

            # Rate Limit Aman: 60 req/min (1 req per 1.05 detik)
            time.sleep(1.05)
            total += 1

    print(f"\nDone! Total desa di-sync: {total}")

if __name__ == "__main__":
    process_all_files()
