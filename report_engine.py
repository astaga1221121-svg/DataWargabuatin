# backend/report_engine.py
import os
import json
import uuid
from datetime import datetime

LAPORAN_DIR = os.path.join(os.path.dirname(__file__), 'laporan')
if not os.path.exists(LAPORAN_DIR):
    os.makedirs(LAPORAN_DIR)

def simpan_laporan(data):
    filename = f"{uuid.uuid4()}.json"
    filepath = os.path.join(LAPORAN_DIR, filename)
    laporan_data = {
        "id": filename.replace(".json", ""),
        "foto": data.get("foto"),
        "lat": data.get("lat"),
        "lon": data.get("lon"),
        "lokasi": data.get("lokasi"),
        "kategori": data.get("kategori"),
        "deskripsi": data.get("deskripsi", ""),
        "waktu": data.get("waktu", datetime.utcnow().isoformat()),
        "kontak": data.get("kontak", ""),
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(laporan_data, f, ensure_ascii=False, indent=2)
    return laporan_data

def semua_laporan():
    hasil = []
    for f in os.listdir(LAPORAN_DIR):
        if f.endswith('.json'):
            with open(os.path.join(LAPORAN_DIR, f), encoding='utf-8') as file:
                hasil.append(json.load(file))
    return hasil
