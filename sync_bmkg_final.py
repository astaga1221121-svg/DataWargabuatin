import os
import json
import time
import requests
import shutil
from datetime import datetime, timedelta, timezone
from supabase import create_client

# Konfigurasi Supabase
SUPABASE_URL = "https://buhkgtkzglgzxosdlqoj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DATA_DIR = "data"
CACHE_DIR = "cache"
TRASH_DIR = "sampahku"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

def fetch_bmkg_data(adm4):
    url = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  [Error Fetch] {adm4}: {e}")
    return None

def process_weather_data(data):
    """Memproses data untuk tampilan realtime di Frontend."""
    if not data or 'data' not in data or not data['data']: return None
    forecast_list = data['data'][0].get('cuaca', [])
    if not forecast_list: return None

    now = datetime.now(timezone.utc)
    closest_slot = None
    min_diff = float('inf')
    today_temps = []

    for day in forecast_list:
        for slot in day:
            try:
                slot_time = datetime.strptime(slot['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                diff = abs((slot_time - now).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_slot = slot
                if slot_time.date() == now.date() and slot.get('t'):
                    today_temps.append(float(slot['t']))
            except: continue

    if not closest_slot: return None

    return {
        "suhu_realtime": float(closest_slot.get('t', 0)),
        "kelembapan_realtime": float(closest_slot.get('hu', 0)),
        "weather_desc": closest_slot.get('weather_desc', 'Berawan'),
        "weather_icon_url": closest_slot.get('image', ''),
        "suhu_max": max(today_temps) if today_temps else 0,
        "suhu_min": min(today_temps) if today_temps else 0,
        "forecast_full": data # Mengirim seluruh data 3 hari ke Supabase
    }

def sync_file(filename):
    json_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(json_path): return

    with open(json_path, 'r', encoding='utf-8') as f:
        villages = json.load(f)

    batch_upsert = []
    for v in villages:
        adm4 = v.get('adm4')
        if not adm4: continue

        new_data = fetch_bmkg_data(adm4)
        if not new_data: continue

        cache_path = os.path.join(CACHE_DIR, f"{adm4}.json")
        
        # Logika pemindahan ke sampahku jika ada update
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as cf:
                try:
                    old_data = json.load(cf)
                    if old_data != new_data:
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        shutil.move(cache_path, os.path.join(TRASH_DIR, f"{adm4}_{ts}.json"))
                except: pass

        with open(cache_path, 'w', encoding='utf-8') as cf:
            json.dump(new_data, cf)

        processed = process_weather_data(new_data)
        if processed:
            processed.update({
                "adm4": adm4, 
                "desa": v.get('desa'), 
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
            batch_upsert.append(processed)

        time.sleep(1.1) # Mematuhi batas 60 req/menit

        if len(batch_upsert) >= 10:
            supabase.table("cuaca_realtime").upsert(batch_upsert).execute()
            batch_upsert = []

    if batch_upsert:
        supabase.table("cuaca_realtime").upsert(batch_upsert).execute()

def cleanup_trash():
    """Menghapus file di sampahku yang lebih dari 1 jam."""
    if not os.path.exists(TRASH_DIR): return
    now = datetime.now()
    for f in os.listdir(TRASH_DIR):
        fpath = os.path.join(TRASH_DIR, f)
        if os.path.isfile(fpath):
            if now - datetime.fromtimestamp(os.path.getmtime(fpath)) > timedelta(hours=1):
                os.remove(fpath)

if __name__ == "__main__":
    import sys
    cleanup_trash()
    if len(sys.argv) > 1:
        sync_file(sys.argv[1])
