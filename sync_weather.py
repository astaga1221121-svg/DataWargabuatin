import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Konfigurasi Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("!!! ERROR: SUPABASE_URL atau SUPABASE_KEY tidak ditemukan di Environment Variables !!!")

DATA_DIR = "data"
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

session = requests.Session()

def save_to_supabase(data, adm4, village_info):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates" # Ini untuk UPSERT
    }

    try:
        # 1. Parsing data BMKG
        data_block = data.get("data", [{}])[0]
        root_loc = data.get("lokasi", {})
        inner_loc = data_block.get("lokasi", {})
        loc = {**inner_loc, **root_loc}

        # 2. UPSERT Master Lokasi
        lokasi_payload = {
            "adm4": str(adm4),
            "desa": loc.get("desa", village_info.get("desa")),
            "kecamatan": loc.get("kecamatan", village_info.get("kecamatan")),
            "kotkab": loc.get("kotkab", village_info.get("kotkab")),
            "provinsi": loc.get("provinsi", village_info.get("provinsi")),
            "lat": float(loc.get("lat", village_info.get("lat", 0))),
            "lon": float(loc.get("lon", village_info.get("lon", 0))),
            "url_bmkg": village_info.get("url")
        }

        res_lokasi = session.post(f"{SUPABASE_URL}/rest/v1/lokasi", headers=headers, json=lokasi_payload)
        if res_lokasi.status_code not in [200, 201, 204]:
            print(f"   [LOKASI ERROR] {adm4}: {res_lokasi.text}")

        # 3. Parsing Cuaca
        cuaca_blocks = data_block.get("cuaca", [])
        cuaca_now = cuaca_blocks[0][0] if cuaca_blocks and cuaca_blocks[0] else {}
        all_points = [item for sublist in cuaca_blocks for item in sublist] if cuaca_blocks else []
        temps = [p.get("t") for p in all_points if p.get("t") is not None]
        hus = [p.get("hu") for p in all_points if p.get("hu") is not None]
        avg_t = sum(temps) / len(temps) if temps else None
        avg_h = sum(hus) / len(hus) if hus else None

        # 4. UPSERT Cuaca Realtime
        cuaca_payload = {
            "adm4": str(adm4),
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

        res_cuaca = session.post(f"{SUPABASE_URL}/rest/v1/cuaca_realtime", headers=headers, json=cuaca_payload)
        if res_cuaca.status_code not in [200, 201, 204]:
            print(f"   [CUACA ERROR] {adm4}: {res_cuaca.text}")
        else:
            print(f"   [SUCCESS] Data {lokasi_payload['desa']} tersimpan ke Supabase.")

    except Exception as e:
        print(f"   [EXCEPTION] {adm4}: {e}")

def process():
    if not os.path.exists(DATA_DIR): return
    json_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json') and f not in ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json']])

    for json_file in json_files:
        print(f"\nProcessing File: {json_file}")
        try:
            with open(os.path.join(DATA_DIR, json_file), 'r', encoding='utf-8') as f:
                villages = json.load(f)
        except: continue

        for v in villages:
            adm4 = v.get('adm4')
            if not adm4: continue

            cache_path = os.path.join(CACHE_DIR, f"{adm4}.json")

            # Fetch BMKG
            try:
                res = session.get(v['url'], timeout=10)
                if res.status_code != 200:
                    time.sleep(1.05)
                    continue
                new_data = res.json()
            except:
                time.sleep(1.05)
                continue

            # Cek Cache (Jika data sama, lewati kirim ke Supabase untuk hemat resource)
            if os.path.exists(cache_path):
                with open(cache_path, 'r') as f:
                    if json.load(f) == new_data:
                        print(f" - {v.get('desa')}: No changes (Skipped Supabase)")
                        time.sleep(1.05)
                        continue

            # Save Cache & Push
            with open(cache_path, 'w') as f: json.dump(new_data, f)
            save_to_supabase(new_data, adm4, v)
            time.sleep(1.05) # Rate limit BMKG

if __name__ == "__main__":
    process()
