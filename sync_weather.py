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

def format_timestamp(date_str):
    """Mengubah format 'YYYY-MM-DD HH:MM:SS' menjadi ISO Format untuk Postgres"""
    try:
        if not date_str: return None
        return date_str.replace(" ", "T")
    except:
        return date_str

def save_bulk_to_supabase(all_slots, adm4, loc_info, village_info):
    if not SUPABASE_URL or not SUPABASE_KEY or not all_slots: return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    payloads = []
    for slot in all_slots:
        try:
            # Parse waktu ke format ISO agar valid di kolom timestamp
            l_dt = format_timestamp(slot.get("local_datetime"))
            if not l_dt: continue

            payloads.append({
                "adm4": str(adm4),
                "local_datetime": l_dt,
                "adm1": loc_info.get("adm1"),
                "adm2": loc_info.get("adm2"),
                "adm3": loc_info.get("adm3"),
                "provinsi": loc_info.get("provinsi", village_info.get("provinsi")),
                "kotkab": loc_info.get("kotkab", village_info.get("kotkab")),
                "kecamatan": loc_info.get("kecamatan", village_info.get("kecamatan")),
                "desa": loc_info.get("desa", village_info.get("desa")),
                "lat": float(loc_info.get("lat", 0)),
                "lon": float(loc_info.get("lon", 0)),
                "suhu_realtime": slot.get("t"),
                "kelembapan_realtime": slot.get("hu"),
                "weather_desc": slot.get("weather_desc"),
                "weather_icon_url": slot.get("image"),
                # VARIABLE TAMBAHAN DARI BMKG
                "rain_mm": slot.get("tp"),
                "wind_speed": slot.get("ws"),
                "wind_dir": slot.get("wd"),
                "cloud_cover": slot.get("tcc"),
                "weather_code": slot.get("weather"),
                "visibility": slot.get("vs"),
                "updated_at": datetime.now().isoformat()
            })
        except: continue

    if not payloads: return

    # Upsert Master Lokasi
    try:
        master = {k: v for k, v in payloads[0].items() if k in ['adm4','desa','kecamatan','kotkab','provinsi','lat','lon']}
        master["url_bmkg"] = village_info.get("url")
        session.post(f"{SUPABASE_URL}/rest/v1/lokasi", headers=headers, json=master)
    except: pass

    # Bulk Upsert Data Cuaca (Otomatis handle (adm4, local_datetime) unik)
    try:
        res = session.post(f"{SUPABASE_URL}/rest/v1/cuaca_realtime", headers=headers, json=payloads)
        return res.status_code in [200, 201, 204]
    except: return False

def process():
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json') and f not in ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json', 'fix_json.py']])

    for f_name in files:
        print(f"\nProcessing File: {f_name}")
        with open(os.path.join(DATA_DIR, f_name), 'r', encoding='utf-8') as f:
            try: villages = json.load(f)
            except: continue

        for v in villages:
            adm4 = v.get('adm4')
            if not adm4: continue

            try:
                res = session.get(v['url'], timeout=12)
                if res.status_code != 200:
                    time.sleep(1.05)
                    continue
                data = res.json()

                data_list = data.get("data", [])
                if not data_list: continue

                loc_info = data_list[0].get("lokasi", {})
                cuaca_days = data_list[0].get("cuaca", [])

                # Kumpulkan semua slot waktu dari 3 hari
                all_forecast_slots = [slot for day in cuaca_days for slot in day]

                if save_bulk_to_supabase(all_forecast_slots, adm4, loc_info, v):
                    print(f" - {v.get('desa')}: OK ({len(all_forecast_slots)} slots)")
                else:
                    print(f" - {v.get('desa')}: FAILED to database")

                time.sleep(1.05)
            except Exception as e:
                print(f" - Error {adm4}: {e}")
                time.sleep(1.05)

if __name__ == "__main__":
    process()
