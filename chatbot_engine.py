# chatbot_engine.py
import os
import json
import re
from typing import Dict, List, Optional, Any, Tuple
from fuzzywuzzy import fuzz, process

class ChatbotEngine:
    def __init__(self):
        self.lokasi_data = {}
        self.hewan_data = []
        self.sayuran_data = []
        self.lokasi_index = {
            'provinsi': {}, 'kotkab': {}, 'kecamatan': {}, 'desa': {}, 'alias': {}
        }
        self.loaded = False
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DATA_FILTERED_DIR = os.path.join(self.BASE_DIR, "data_filtered")
        self.DATA_DIR = os.path.join(self.BASE_DIR, "data")
        self.load_data()

    def load_data(self):
        try:
            self._load_lokasi_data()
            self._load_hewan_data()
            self._load_sayuran_data()
            self.loaded = True
            print(f"✅ ChatbotEngine: {len(self.lokasi_data)} lokasi dimuat.")
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            self.loaded = False

    def load_filtered_data(self):
        self.lokasi_data.clear()
        for key in self.lokasi_index: self.lokasi_index[key].clear()
        self.loaded = False
        self.load_data()

    def _load_lokasi_data(self):
        if not os.path.exists(self.DATA_FILTERED_DIR): return
        files = [f for f in os.listdir(self.DATA_FILTERED_DIR) if f.endswith('.json')]
        for filename in files:
            try:
                with open(os.path.join(self.DATA_FILTERED_DIR, filename), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Pastikan data memiliki field yang dibutuhkan
                p, k, kec, d = data.get('provinsi'), data.get('kotkab'), data.get('kecamatan'), data.get('desa')
                if all([p, k, kec, d]):
                    key = f"{p}_{k}_{kec}_{d}"
                    self.lokasi_data[key] = data
                    self.lokasi_index['provinsi'].setdefault(p.lower(), []).append(key)
                    self.lokasi_index['kotkab'].setdefault(k.lower(), []).append(key)
                    self.lokasi_index['kecamatan'].setdefault(kec.lower(), []).append(key)
                    self.lokasi_index['desa'].setdefault(d.lower(), []).append(key)
            except: continue

    def _load_hewan_data(self):
        path = os.path.join(self.DATA_DIR, "hewan_cocok.json")
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f: self.hewan_data = json.load(f)

    def _load_sayuran_data(self):
        path = os.path.join(self.DATA_DIR, "sayuran_cocok.json")
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f: self.sayuran_data = json.load(f)

    def process_query(self, question: str) -> str:
        if not self.loaded: return "Maaf, data sedang disiapkan. Tunggu sebentar."
        q = question.lower().strip()

        if 'provinsi apa saja' in q:
            items = sorted(list(set(d['provinsi'] for d in self.lokasi_data.values())))
            return f"Provinsi tersedia: {', '.join(items)}" if items else "Data tidak ditemukan."

        if 'kota apa saja' in q or 'kotkab apa saja' in q:
            items = sorted(list(set(d['kotkab'] for d in self.lokasi_data.values())))
            return f"Kota/Kabupaten tersedia: {', '.join(items)}" if items else "Data tidak ditemukan."

        if 'kecamatan apa saja' in q:
            items = sorted(list(set(d['kecamatan'] for d in self.lokasi_data.values())))
            return f"Kecamatan tersedia: {', '.join(items)}" if items else "Data tidak ditemukan."

        if 'desa apa saja' in q:
            items = sorted(list(set(d['desa'] for d in self.lokasi_data.values())))
            return f"Desa tersedia: {', '.join(items)}" if items else "Data tidak ditemukan."

        if 'hewan' in q and ('apa saja' in q or 'daftar' in q):
            items = [h['nama'] for h in self.hewan_data]
            return f"Hewan yang dapat dicek: {', '.join(items)}"

        if 'sayuran' in q and ('apa saja' in q or 'daftar' in q):
            items = [s['nama'] for s in self.sayuran_data]
            return f"Sayuran yang tersedia: {', '.join(items)}"

        # Default handler untuk cuaca di [lokasi]
        match = re.search(r'cuaca di (.+)', q)
        if match:
            loc_name = match.group(1).strip()
            for level in ['desa', 'kecamatan', 'kotkab']:
                if loc_name in self.lokasi_index[level]:
                    key = self.lokasi_index[level][loc_name][0]
                    data = self.lokasi_data[key]
                    return f"Cuaca di {data['desa']} ({data['kecamatan']}) saat ini {data['cuaca_saat_ini']['cuaca']} dengan suhu {data['cuaca_saat_ini']['suhu']}°C."
            return f"Maaf, lokasi '{loc_name}' tidak ditemukan."

        return "Maaf, saya belum mengerti pertanyaan itu. Coba tanyakan 'Daftar hewan' atau 'Cuaca di [lokasi]'."
