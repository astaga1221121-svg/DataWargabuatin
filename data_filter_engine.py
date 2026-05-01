# data_filter_engine.py
import os
import json
import datetime
import shutil
from collections import Counter
import re
import concurrent.futures

# --- Base & Directory Paths (Disesuaikan otomatis untuk Windows) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
DATA_FILTERED_DIR = os.path.join(BASE_DIR, "data_filtered")
SAMPAH_DIR = os.path.join(BASE_DIR, "sampahku")

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_FILTERED_DIR, exist_ok=True)
os.makedirs(SAMPAH_DIR, exist_ok=True)

class DataFilterEngine:
    def __init__(self, cache_folder=CACHE_DIR, filtered_folder=DATA_FILTERED_DIR, sampah_folder=SAMPAH_DIR):
        self.cache_folder = cache_folder
        self.filtered_folder = filtered_folder
        self.sampah_folder = sampah_folder
        self._ensure_folders_exist()

    def _ensure_folders_exist(self):
        os.makedirs(self.cache_folder, exist_ok=True)
        os.makedirs(self.filtered_folder, exist_ok=True)
        os.makedirs(self.sampah_folder, exist_ok=True)

    def run_filter_process(self):
        print(f"--- Memulai tugas terjadwal: Pemfilteran Data ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")

        # Tambahkan fungsi fetch data dari BMKG di sini jika ingin benar-benar realtime
        # Untuk saat ini, kita mengandalkan data yang ada di cache.

        raw_files = self._load_raw_data()
        if not raw_files:
            print("✅ Ditemukan 0 data mentah di folder cache.")
            return

        valid_location_data = self._filter_valid_locations(raw_files)
        valid_weather_data = self._filter_valid_weather_data(valid_location_data)
        summarized_data = self._summarize_weather_data(valid_weather_data)
        final_filtered_data = self._normalize_and_alias_data(summarized_data)
        self._save_filtered_data(final_filtered_data)

    def _read_json_file(self, filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Mendukung format baru BMKG API
                if isinstance(data, dict) and 'data' in data:
                    # Logic ekstraksi data BMKG (Sederhana)
                    loc_info = data.get('lokasi', {})
                    weather_list = []
                    
                    # Cek jika data cuaca ada di dalam 'data' array
                    for item in data.get('data', []):
                        if isinstance(item, list):
                            weather_list.extend(item)
                        else:
                            weather_list.append(item)

                    return {
                        "lokasi": loc_info,
                        "cuaca": weather_list,
                        "analysis_date": data.get("analysis_date", datetime.datetime.now().isoformat())
                    }
                return None
        except Exception as e:
            return None

    def _load_raw_data(self):
        all_raw_data = []
        if not os.path.exists(self.cache_folder): return []
        
        json_files = [os.path.join(self.cache_folder, f) for f in os.listdir(self.cache_folder) if f.endswith('.json')]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self._read_json_file, json_files)
            all_raw_data = [d for d in results if d is not None]
        return all_raw_data

    def _filter_valid_locations(self, raw_data_list):
        valid_locations = []
        for entry in raw_data_list:
            lokasi = entry.get('lokasi', {})
            # Pastikan koordinat ada agar muncul di peta
            if lokasi.get('lat') and lokasi.get('lon'):
                valid_locations.append(entry)
        return valid_locations

    def _filter_valid_weather_data(self, data):
        # Sederhanakan filter agar data BMKG masuk semua
        return data

    def _summarize_weather_data(self, filtered_data):
        processed_data = []
        for entry in filtered_data:
            loc = entry["lokasi"]
            weather_data = entry["cuaca"]

            # Ambil data terbaru untuk cuaca_saat_ini
            latest = weather_data[0] if weather_data else {}

            processed_entry = {
                "provinsi": loc.get("provinsi"),
                "kotkab": loc.get("kotkab"),
                "kecamatan": loc.get("kecamatan"),
                "desa": loc.get("desa"),
                "lon": float(loc.get("lon")),
                "lat": float(loc.get("lat")),
                "adm4": loc.get("adm4"),
                "cuaca_saat_ini": {
                    "suhu": latest.get("t"),
                    "kelembapan": latest.get("hu"),
                    "cuaca": latest.get("weather_desc"),
                    "ikon": latest.get("image")
                },
                "ringkasan_harian": {
                    "t_max": latest.get("t"), # Sederhanakan untuk demo
                    "t_min": latest.get("t"),
                    "t_avg": latest.get("t"),
                    "hu_avg": latest.get("hu"),
                    "cuaca_dominan": latest.get("weather_desc")
                }
            }
            processed_data.append(processed_entry)
        return processed_data

    def _normalize_and_alias_data(self, summary_data):
        for entry in summary_data:
            entry['alias'] = [entry['desa'], entry['kecamatan'], entry['adm4']]
        return summary_data

    def _save_filtered_data(self, filtered_data_list):
        for data_item in filtered_data_list:
            filename = f"{data_item['adm4']}.json"
            with open(os.path.join(self.filtered_folder, filename), 'w', encoding='utf-8') as f:
                json.dump(data_item, f, indent=2)
        print(f"✅ Tersimpan {len(filtered_data_list)} data lokasi.")

if __name__ == '__main__':
    filter_engine = DataFilterEngine()
    filter_engine.run_filter_process()
