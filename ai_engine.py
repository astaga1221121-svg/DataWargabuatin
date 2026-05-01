# ai_engine.py (Server)
import os
import json
import datetime
import requests
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, 'cache')
DATA_DIR = os.path.join(BASE_DIR, 'data')
SAMPAH_DIR = os.path.join(BASE_DIR, 'sampahku')

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(SAMPAH_DIR, exist_ok=True)

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except: return []

def save_cache(adm4, data):
    """Saves data to a JSON cache file in the CACHE_DIR."""
    file_path = os.path.join(CACHE_DIR, f"{adm4}.json")
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"❌ Failed to save cache {adm4}: {e}")
        return False

def process_and_save_cache(payload):
    """Memproses payload dari fetcher eksternal (jika ada) dan menyimpannya ke cache."""
    if not payload or 'lokasi' not in payload or 'data' not in payload:
        return {"status": "error", "message": "Invalid payload"}, 400

    adm4 = payload['lokasi'].get('adm4')
    if not adm4:
        return {"status": "error", "message": "'adm4' not found"}, 400

    if save_cache(adm4, payload):
        return {"status": "success", "message": f"Cache for {adm4} updated."}, 200
    return {"status": "error", "message": "Save failed"}, 500

def fetch_bmkg_data_realtime():
    """Mengambil data BMKG secara otomatis berdasarkan daftar URL di folder data/."""
    print("🌐 Memulai Sinkronisasi Realtime BMKG...")
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".json") and filename not in ['hewan_cocok.json', 'sayuran_cocok.json']:
            locations = load_json(os.path.join(DATA_DIR, filename))
            if not isinstance(locations, list): continue

            for loc in locations:
                adm4 = loc.get('adm4')
                url = loc.get('url')
                if adm4 and url:
                    try:
                        resp = requests.get(url, timeout=15)
                        if resp.status_code == 200:
                            data = resp.json()

                            # Pastikan koordinat dari file sumber (Jambi.json dsb) ikut tersimpan
                            if 'data' in data and len(data['data']) > 0:
                                if isinstance(data['data'][0], dict):
                                    if 'lokasi' not in data['data'][0]:
                                        data['data'][0]['lokasi'] = loc
                                    else:
                                        # Gabungkan metadata lokasi lokal ke dalam payload BMKG
                                        data['data'][0]['lokasi'].update(loc)

                            save_cache(adm4, data)
                            print(f"✅ Synced: {loc.get('desa')} ({adm4})")
                    except Exception as e:
                        print(f"❌ Error sync {adm4}: {e}")
