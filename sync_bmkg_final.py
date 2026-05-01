import os
import json
import time
import requests
import shutil
from datetime import datetime, timedelta, timezone
from supabase import create_client

# Konfigurasi
URL = "https://buhkgtkzglgzxosdlqoj.supabase.co"
KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(URL, KEY)

BASE_DIR = "DataWargabuatin"
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_DIR = os.path.join(BASE_DIR, "cache")
TRASH_DIR = os.path.join(BASE_DIR, "sampahku")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

def fetch_weather(adm4):
    api_url = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}"
    try:
        resp = requests.get(api_url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except: return None
    return None

def sync_file(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path): return
    
    with open(path, 'r', encoding='utf-8') as f:
        villages = json.load(f)

    batch_upsert = []
    for v in villages:
        adm4 = v['adm4']
        new_data = fetch_weather(adm4)
        if not new_data: continue

        cache_path = os.path.join(CACHE_DIR, f"{adm4}.json")
        
        # Logika Update & Trash
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as cf:
                old_data = json.load(cf)
            if old_data != new_data:
                # Pindahkan ke sampah jika ada perubahan
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                shutil.move(cache_path, os.path.join(TRASH_DIR, f"{adm4}_{ts}.json"))
        
        # Simpan ke Cache
        with open(cache_path, 'w') as cf:
            json.dump(new_data, cf)

        # Siapkan data untuk Supabase
        # Mengambil slot waktu terdekat (index 0 cuaca hari ini)
        try:
            current = new_data['data'][0]['cuaca'][0][0] # Slot terdekat
            batch_upsert.append({
                "adm4": adm4,
                "suhu_realtime": float(current['t']),
                "kelembapan_realtime": float(current['hu']),
                "weather_desc": current['weather_desc'],
                "weather_icon_url": current['image'],
                "forecast_full": new_data, # Data 3 hari lengkap masuk ke JSONB
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
        except: continue

        # Rate limit 60req/min = 1 req per detik
        time.sleep(1.1)

        # Batch push ke Supabase setiap 10 desa
        if len(batch_upsert) >= 10:
            supabase.table("cuaca_realtime").upsert(batch_upsert).execute()
            batch_upsert = []

    if batch_upsert:
        supabase.table("cuaca_realtime").upsert(batch_upsert).execute()

def cleanup_trash():
    # Hapus file di sampahku yang > 1 jam
    now = datetime.now()
    for f in os.listdir(TRASH_DIR):
        fpath = os.path.join(TRASH_DIR, f)
        if os.path.getmtime(fpath) < (now - timedelta(hours=1)).timestamp():
            os.remove(fpath)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        sync_file(sys.argv[1])
        cleanup_trash()