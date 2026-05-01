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
if not SUPABASE_KEY:
    # Key cadangan hanya jika dijalankan lokal
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJ1aGtndGt6Z2xnenhvc2RscW9qIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzU4NzQ3OCwiZXhwIjoyMDkzMTYzNDc4fQ.94EY7AQGV9cQqezrkGxX3csBp4XJH4CdaZ4LGEfehBI"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Folder Logic sesuai struktur baru Anda
BASE_DIR = "DataWargabuatin"
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_DIR = os.path.join(BASE_DIR, "cache")
TRASH_DIR = os.path.join(BASE_DIR, "sampahku")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

def fetch_bmkg_data(adm4):
    url = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}"
    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"  [Error Fetch] {adm4}: {e}")
    return None

def process_weather_data(data):
    if not data or 'data' not in data: return None
    forecast_list = data['data'][0].get('cuaca', [])
    if not forecast_list: return None

    now = datetime.now(timezone.utc)
    closest_slot = None
    min_diff = float('inf')
    today_slots = []

    for day_forecast in forecast_list:
        for slot in day_forecast:
            try:
                slot_time = datetime.strptime(slot['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                diff = abs((slot_time - now).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_slot = slot
                if slot_time.date() == now.date():
                    today_slots.append(slot)
            except: continue

    if not closest_slot: return None
    temps = [float(s['t']) for s in today_slots if s.get('t')]
    humids = [float(s['hu']) for s in today_slots if s.get('hu')]

    return {
        "suhu_realtime": float(closest_slot.get('t', 0)),
        "kelembapan_realtime": float(closest_slot.get('hu', 0)),
        "weather_desc": closest_slot.get('weather_desc', 'Berawan'),
        "weather_icon_url": closest_slot.get('image', ''),
        "rata2_suhu": sum(temps)/len(temps) if temps else 0,
        "rata2_hu": sum(humids)/len(humids) if humids else 0,
        "suhu_max_hari_ini": max(temps) if temps else 0,
        "suhu_min_hari_ini": min(temps) if temps else 0,
        "suhu_rata2_hari_ini": sum(temps)/len(temps) if temps else 0,
        "forecast_full": data
    }

def sync_file(filename):
    # Filename hanya Aceh.json, kita gabung dengan DATA_DIR
    json_file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(json_file_path):
        print(f"❌ File tidak ditemukan: {json_file_path}")
        return

    print(f"🚀 Memproses file: {json_file_path}")
    with open(json_file_path, 'r', encoding='utf-8') as f:
        villages = json.load(f)

    batch_supabase = []
    for i, v in enumerate(villages):
        adm4 = v['adm4']
        new_raw = fetch_bmkg_data(adm4)
        if not new_raw: continue

        cache_path = os.path.join(CACHE_DIR, f"{adm4}.json")
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as cf:
                old_raw = json.load(cf)
            if old_raw != new_raw:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                trash_path = os.path.join(TRASH_DIR, f"{adm4}_{ts}.json")
                shutil.move(cache_path, trash_path)

        with open(cache_path, 'w') as cf:
            json.dump(new_raw, cf)

        processed = process_weather_data(new_raw)
        if processed:
            processed["adm4"] = adm4
            processed["updated_at"] = datetime.now(timezone.utc).isoformat()
            batch_supabase.append(processed)

        time.sleep(1.1) # Batasi 60 req/min
        if len(batch_supabase) >= 10:
            supabase.table("cuaca_realtime").upsert(batch_supabase).execute()
            batch_supabase = []

    if batch_supabase:
        supabase.table("cuaca_realtime").upsert(batch_supabase).execute()

def cleanup_trash():
    now = datetime.now()
    if not os.path.exists(TRASH_DIR): return
    for f in os.listdir(TRASH_DIR):
        fpath = os.path.join(TRASH_DIR, f)
        if os.path.isfile(fpath):
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if now - mtime > timedelta(hours=1):
                os.remove(fpath)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        sync_file(sys.argv[1])
        cleanup_trash()
