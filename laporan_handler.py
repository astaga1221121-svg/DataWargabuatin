# laporan_handler.py
import os
import json
import uuid # Pastikan uuid diimpor
from datetime import datetime # Impor datetime untuk timestamp

# --- PERUBAHAN DI SINI ---
# Asumsi smart_rekomendasi SEKARANG ada di recommendation_module.py
from recommendation_module import smart_rekomendasi
# --- AKHIR PERUBAHAN ---

# Direktori tempat menyimpan file laporan
LAPORAN_DIR = os.path.join(os.path.dirname(__file__), 'laporan')

# Pastikan folder 'laporan' ada. Jika tidak, akan dibuat.
if not os.path.exists(LAPORAN_DIR):
    os.makedirs(LAPORAN_DIR)

# Koordinat default untuk pusat Indonesia, akan digunakan jika lokasi tidak ditemukan
DEFAULT_LAT = -2.0
DEFAULT_LON = 118.0

def simpan_laporan(data_laporan):
    """
    Menyimpan data laporan ke dalam file JSON terpisah di folder LAPORAN_DIR.
    data_laporan: dictionary yang berisi detail laporan dari frontend.
    """
    lokasi_nama = data_laporan.get('lokasi', '').strip()
    
    # Inisialisasi lat, lon dengan nilai dari data_laporan (jika sudah ada dari frontend)
    # Ini penting agar koordinat yang sudah di-geocode di frontend tidak ditimpa
    lat = data_laporan.get('lat')
    lon = data_laporan.get('lon')

    # Jika lokasi nama diberikan dan lat/lon belum diisi (misal geocoding frontend gagal)
    # atau jika smart_rekomendasi dapat memberikan koordinat yang lebih akurat
    if lokasi_nama and (lat is None or lon is None):
        try:
            # Panggil smart_rekomendasi untuk mendapatkan koordinat berdasarkan nama lokasi
            # Asumsi smart_rekomendasi mengembalikan list dari dict, dan hasil pertama adalah yang terbaik
            hasil_rekomendasi = smart_rekomendasi(lokasi_nama)
            if hasil_rekomendasi and isinstance(hasil_rekomendasi, list) and len(hasil_rekomendasi) > 0:
                # Ambil lat/lon dari hasil rekomendasi pertama
                rec_lat = hasil_rekomendasi[0].get('lat')
                rec_lon = hasil_rekomendasi[0].get('lon')
                
                # Gunakan koordinat dari rekomendasi jika valid
                if rec_lat is not None and rec_lon is not None:
                    lat = float(rec_lat)
                    lon = float(rec_lon)
                    print(f"DEBUG: Lokasi '{lokasi_nama}' di-geocode ulang di backend: {lat}, {lon}")
        except Exception as e:
            print(f"WARNING: Gagal geocoding '{lokasi_nama}' di backend: {e}")
            # Biarkan lat/lon tetap None jika ada error, agar fallback ke default

    # Pastikan lat dan lon memiliki nilai. Jika masih None setelah upaya geocoding, gunakan default.
    data_laporan['lat'] = float(lat) if lat is not None else DEFAULT_LAT
    data_laporan['lon'] = float(lon) if lon is not None else DEFAULT_LON

    # Tambahkan timestamp jika belum ada (dari frontend)
    if 'waktu' not in data_laporan:
        data_laporan['waktu'] = datetime.now().isoformat()

    # Gunakan uuid untuk nama file yang unik
    filename = f"{uuid.uuid4()}.json"
    filepath = os.path.join(LAPORAN_DIR, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data_laporan, f, ensure_ascii=False, indent=2)
    
    print(f"Laporan berhasil disimpan dengan UUID {filename} di {filepath}")