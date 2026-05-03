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

def save_bulk_to_supabase(all_slots, adm4, loc_info, village_info):
    """Mengirim seluruh slot waktu sekaligus ke Supabase"""
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
            # Pastikan data suhu ada, jika tidak ada skip slot ini saja
            if slot.get("t") is None: continue

            payloads.append({
                "adm4": str(adm4),
                "local_datetime": slot.get("local_datetime"),
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
                "updated_at": datetime.now().isoformat()
            })
        except: continue

    if not payloads: return

    # Update Master Lokasi dulu (sekali saja)
    try:
        master_payload = {
            "adm4": str(adm4),
            "desa": payloads[0]["desa"],
            "kecamatan": payloads[0]["kecamatan"],
            "kotkab": payloads[0]["kotkab"],
            "provinsi": payloads[0]["provinsi"],
            "lat": payloads[0]["lat"],
            "lon": payloads[0]["lon"],
            "url_bmkg": village_info.get("url")
        }
        session.post(f"{SUPABASE_URL}/rest/v1/lokasi", headers=headers, json=master_payload)
    except: pass

    # Bulk Upsert Data Cuaca
    try:
        res = session.post(f"{SUPABASE_URL}/rest/v1/cuaca_realtime", headers=headers, json=payloads)
        return res.status_code in [200, 201, 204]
    except: return False

def process():
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.json') and f not in ['links_api.json', 'hewan_cocok.json', 'sayuran_cocok.json', 'new_kecamatanss.json', 'fix_json.py']])

    for f_name in files:
        print(f"\nWilayah: {f_name}")
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

                # Bongkar data[0].cuaca yang merupakan list of lists
                all_forecast_slots = []
                data_list = data.get("data", [])
                if not data_list: continue

                loc_info = data_list[0].get("lokasi", {})
                cuaca_days = data_list[0].get("cuaca", [])

                for day in cuaca_days:
                    for slot in day:
                        all_forecast_slots.append(slot)

                # Kirim semua slot sekaligus
                if save_bulk_to_supabase(all_forecast_slots, adm4, loc_info, v):
                    print(f" - {v.get('desa')}: {len(all_forecast_slots)} slot di-sync")
                else:
                    print(f" - {v.get('desa')}: Gagal sync")

                time.sleep(1.05) # Rate limit BMKG
            except:
                time.sleep(1.05)

if __name__ == "__main__":
    process()
