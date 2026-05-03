import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

DATA_DIR = "data"
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=3)
session.mount("https://", adapter)
session.mount("http://", adapter)

def calculate_stats(forecast_days):
    """
    Ekstrak semua titik data cuaca dari struktur nested BMKG
    Format: data[0].cuaca[hari][jam]
    """
    all_points = []
    for day in forecast_days:
        for hour_data in day:
            all_points.append(hour_data)

    if not all_points:
        return None

    temps = [p.get("t") for p in all_points if p.get("t") is not None]
    hus = [p.get("hu") for p in all_points if p.get("hu") is not None]

    if not temps: return None

    return {
        "suhu_max": max(temps),
        "suhu_min": min(temps),
        "suhu_rata": round(sum(temps) / len(temps), 1),
        "hu_rata": round(sum(hus) / len(hus), 0) if hus else None,
        "current": all_points[0] # Titik waktu terdekat
    }

def save_to_supabase(data, adm4, village_info):
    if not SUPABASE_URL or not SUPABASE_KEY: return False

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    try:
        # 1. Parsing struktur utama
        # Kadang BMKG mengembalikan error 404 dalam bentuk JSON
        if "error" in data or "message" in data and data.get("statusCode") == 404:
            return False

        data_list = data.get("data", [])
        if not data_list: return False

        main_data = data_list[0]
        loc_info = main_data.get("lokasi", {})
        forecast_cuaca = main_data.get("cuaca", []) # List of lists (per hari)

        stats = calculate_stats(forecast_cuaca)
        if not stats: return False

        # 2. UPSERT Master Lokasi
        lokasi_payload = {
            "adm4": str(adm4),
            "desa": loc_info.get("desa", village_info.get("desa")),
            "kecamatan": loc_info.get("kecamatan", village_info.get("kecamatan")),
            "kotkab": loc_info.get("kotkab", village_info.get("kotkab")),
            "provinsi": loc_info.get("provinsi", village_info.get("provinsi")),
            "lat": float(loc_info.get("lat", village_info.get("lat", 0))),
            "lon": float(loc_info.get("lon", village_info.get("lon", 0))),
            "url_bmkg": village_info.get("url")
        }
        session.post(f"{SUPABASE_URL}/rest/v1/lokasi", headers=headers, json=lokasi_payload)

        # 3. UPSERT Cuaca Realtime
        cuaca_payload = {
            "adm4": str(adm4),
            "adm1": loc_info.get("adm1"),
            "adm2": loc_info.get("adm2"),
            "adm3": loc_info.get("adm3"),
            "provinsi": lokasi_payload["provinsi"],
            "kotkab": lokasi_payload["kotkab"],
            "kecamatan": lokasi_payload["kecamatan"],
            "desa": lokasi_payload["desa"],
            "lat": lokasi_payload["lat"],
            "lon": lokasi_payload["lon"],
            "suhu_realtime": stats["current"].get("t"),
            "kelembapan_realtime": stats["current"].get("hu"),
            "weather_desc": stats["current"].get("weather_desc"),
            "weather_icon_url": stats["current"].get("image"),
            "rata2_suhu": stats["suhu_rata"],
            "rata2_hu": stats["hu_rata"],
            "suhu_max_hari_ini": stats["suhu_max"],
            "suhu_min_hari_ini": stats["suhu_min"],
            "suhu_rata2_hari_ini": stats["suhu_rata"],
            "forecast_data": data,
            "updated_at": datetime.now().isoformat()
        }

        res = session.post(f"{SUPABASE_URL}/rest/v1/cuaca_realtime", headers=headers, json=cuaca_payload)
        return res.status_code in [200, 201, 204]

    except Exception as e:
        print(f"   [ERR] {adm4}: {e}")
        return False

def process():
    if not os.path.exists(DATA_DIR): return
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json') and f not in ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json', 'fix_json.py']])

    for f_name in files:
        print(f"\nProcessing: {f_name}")
        with open(os.path.join(DATA_DIR, f_name), 'r', encoding='utf-8') as f:
            try: villages = json.load(f)
            except: continue

        for v in villages:
            adm4 = v.get('adm4')
            if not adm4: continue

            try:
                res = session.get(v['url'], timeout=10)
                if res.status_code != 200:
                    time.sleep(1.05)
                    continue
                data = res.json()

                # Cek Cache
                cache_p = os.path.join(CACHE_DIR, f"{adm4}.json")
                if os.path.exists(cache_p):
                    with open(cache_p, 'r') as cf:
                        if json.load(cf) == data:
                            print(f" - {v.get('desa')}: No Change")
                            time.sleep(1.05)
                            continue

                if save_to_supabase(data, adm4, v):
                    with open(cache_p, 'w') as cf: json.dump(data, cf)
                    print(f" - {v.get('desa')}: OK")
                else:
                    print(f" - {v.get('desa')}: FAILED/EMPTY")

                time.sleep(1.05)
            except:
                time.sleep(1.05)

if __name__ == "__main__":
    process()
