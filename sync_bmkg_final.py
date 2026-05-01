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

# Gunakan key cadangan jika env tidak tersedia (hanya untuk pengujian)
if not SUPABASE_KEY:
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJ1aGtndGt6Z2xnenhvc2RscW9qIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzU4NzQ3OCwiZXhwIjoyMDkzMTYzNDc4fQ.94EY7AQGV9cQqezrkGxX3csBp4XJH4CdaZ4LGEfehBI"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Folder Logic - Sesuaikan dengan struktur root repo GitHub
DATA_DIR = "data"
CACHE_DIR = "cache"
TRASH_DIR = "sampahku"

# Pastikan folder ada
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(TRASH_DIR, exist_ok=True)

def fetch_bmkg_data(adm4):
    """Fetch data dari API BMKG berdasarkan kode adm4."""
    url = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"  [Warn] {adm4} returned status {resp.status_code}")
    except Exception as e:
        print(f"  [Error Fetch] {adm4}: {e}")
    return None

def process_weather_data(data):
    """Memproses JSON BMKG menjadi format ringkas untuk Supabase."""
    if not data or 'data' not in data or not data['data']:
        return None

    # BMKG data structure usually has 'data'[0]['cuaca']
    forecast_container = data['data'][0]
    forecast_list = forecast_container.get('cuaca', [])
    if not forecast_list:
        return None

    now = datetime.now(timezone.utc)
    closest_slot = None
    min_diff = float('inf')
    today_slots = []

    # Iterate melalui hari (prakiraan biasanya 3 hari)
    for day_forecast in forecast_list:
        # day_forecast adalah list of slots (per 3 jam)
        for slot in day_forecast:
            try:
                # Format: 2024-05-20 12:00:00
                slot_time = datetime.strptime(slot['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                diff = abs((slot_time - now).total_seconds())

                # Cari data yang paling mendekati waktu sekarang (Realtime)
                if diff < min_diff:
                    min_diff = diff
                    closest_slot = slot

                # Kumpulkan data hari ini saja untuk statistik harian
                if slot_time.date() == now.date():
                    today_slots.append(slot)
            except Exception:
                continue

    if not closest_slot:
        return None

    # Ekstrak suhu dan kelembapan untuk perhitungan rata-rata harian
    temps = [float(s['t']) for s in today_slots if s.get('t') is not None]
    humids = [float(s['hu']) for s in today_slots if s.get('hu') is not None]

    return {
        "suhu_realtime": float(closest_slot.get('t', 0)),
        "kelembapan_realtime": float(closest_slot.get('hu', 0)),
        "weather_desc": closest_slot.get('weather_desc', 'Berawan'),
        "weather_icon_url": closest_slot.get('image', ''),
        "suhu_max_hari_ini": max(temps) if temps else 0,
        "suhu_min_hari_ini": min(temps) if temps else 0,
        "suhu_rata2_hari_ini": sum(temps)/len(temps) if temps else 0,
        "kelembapan_rata2_hari_ini": sum(humids)/len(humids) if humids else 0,
        "forecast_full": data  # Simpan full JSON agar frontend bisa ambil semua data 3 hari
    }

def sync_file(filename):
    """Proses satu file JSON provinsi/kabupaten."""
    # Pastikan filename tidak mengandung path ganda
    base_filename = os.path.basename(filename)
    json_file_path = os.path.join(DATA_DIR, base_filename)

    if not os.path.exists(json_file_path):
        print(f"❌ File tidak ditemukan: {json_file_path}")
        return

    print(f"🚀 Memproses: {json_file_path}")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            villages = json.load(f)
    except Exception as e:
        print(f"❌ Gagal membaca {json_file_path}: {e}")
        return

    batch_supabase = []
    total = len(villages)

    for i, v in enumerate(villages):
        adm4 = v.get('adm4')
        desa = v.get('desa', 'Unknown')
        if not adm4: continue

        print(f"  [{i+1}/{total}] Fetching {desa} ({adm4})...")
        new_raw = fetch_bmkg_data(adm4)

        if not new_raw:
            print(f"  [Skip] {adm4} gagal di-fetch.")
            continue

        cache_path = os.path.join(CACHE_DIR, f"{adm4}.json")

        # Cek apakah ada perubahan data (Update Logic)
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as cf:
                try:
                    old_raw = json.load(cf)
                except:
                    old_raw = None

            # Jika data berbeda, pindahkan yang lama ke sampahku
            if old_raw != new_raw:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                trash_path = os.path.join(TRASH_DIR, f"{adm4}_{ts}.json")
                shutil.move(cache_path, trash_path)
                print(f"  [Update] Data {adm4} berubah, file lama dipindah ke sampahku.")

        # Simpan data terbaru ke cache
        with open(cache_path, 'w', encoding='utf-8') as cf:
            json.dump(new_raw, cf)

        # Proses data untuk diupload ke Supabase
        processed = process_weather_data(new_raw)
        if processed:
            processed["adm4"] = adm4
            processed["desa"] = desa
            processed["updated_at"] = datetime.now(timezone.utc).isoformat()
            batch_supabase.append(processed)

        # Rate Limit: 60 req/min = 1 req/sec. Kita beri jeda 1.1s agar aman.
        time.sleep(1.1)

        # Upsert ke Supabase per batch (misal 10 data sekali jalan)
        if len(batch_supabase) >= 10:
            try:
                supabase.table("cuaca_realtime").upsert(batch_supabase).execute()
                batch_supabase = []
            except Exception as e:
                print(f"  [Supabase Error] {e}")

    # Sisa batch terakhir
    if batch_supabase:
        try:
            supabase.table("cuaca_realtime").upsert(batch_supabase).execute()
        except Exception as e:
            print(f"  [Supabase Error] {e}")

def cleanup_trash():
    """Hapus file di sampahku yang sudah lebih dari 1 jam."""
    now = datetime.now()
    if not os.path.exists(TRASH_DIR):
        return

    deleted_count = 0
    for f in os.listdir(TRASH_DIR):
        fpath = os.path.join(TRASH_DIR, f)
        if os.path.isfile(fpath):
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if now - mtime > timedelta(hours=1):
                try:
                    os.remove(fpath)
                    deleted_count += 1
                except:
                    pass
    if deleted_count > 0:
        print(f"🧹 Cleanup: Berhasil menghapus {deleted_count} file lama di sampahku.")

if __name__ == "__main__":
    import sys
    # Pastikan kita membersihkan sampah setiap kali script jalan
    cleanup_trash()

    if len(sys.argv) > 1:
        sync_file(sys.argv[1])
    else:
        print("Gunakan: python sync_bmkg_final.py <nama_file.json>")
