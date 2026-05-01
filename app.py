from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from datetime import datetime
import threading
import time
import math

# --- BASE DIRECTORY ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Import internal modules ---
from data_filter_engine import DataFilterEngine
from chatbot_engine import ChatbotEngine
from recommendation_module import smart_rekomendasi, skor_cocok_item
from laporan_handler import simpan_laporan
from ai_engine import process_and_save_cache, load_json, fetch_bmkg_data_realtime, DATA_DIR
from cleanup_engine import clean_old_json_files

# --- Initialize Flask App ---
app = Flask(__name__)
CORS(app)

# In-memory cache untuk performa realtime
global_lokasi_cache = []

# Inisialisasi instance
data_filter_instance = DataFilterEngine()
chatbot_instance = ChatbotEngine()

def update_caches():
    global global_lokasi_cache
    try:
        print("🔄 Memperbarui cache data lokasi...")
        global_lokasi_cache = smart_rekomendasi('')
        print(f"✅ Cache diperbarui: {len(global_lokasi_cache)} lokasi siap ditampilkan.")
    except Exception as e:
        print(f"❌ Gagal memperbarui cache: {e}")

# Background task untuk update data BMKG & Filter secara berkala
def scheduled_realtime_task():
    # Sync pertama kali saat startup
    fetch_bmkg_data_realtime()
    data_filter_instance.run_filter_process()
    update_caches()

    while True:
        try:
            time.sleep(900) # Setiap 15 menit
            print("\n[Background] 🔄 Sinkronisasi berkala BMKG...")
            fetch_bmkg_data_realtime()
            data_filter_instance.run_filter_process()
            chatbot_instance.load_filtered_data()
            update_caches()
        except Exception as e:
            print(f"❌ Error background task: {e}")

# Jalankan background thread
threading.Thread(target=scheduled_realtime_task, daemon=True).start()

@app.route('/api/chatbot', methods=['POST'])
def chatbot():
    try:
        user_input = request.json.get('keyword', '').strip()
        jawaban = chatbot_instance.process_query(user_input)
        return jsonify({"jawaban": jawaban})
    except Exception as e:
        return jsonify({"jawaban": f"Maaf, ada kendala: {str(e)}"}), 500

@app.route('/api/all', methods=['GET'])
def all_lokasi():
    # Return langsung dari memori (Sangat Cepat/Realtime)
    return jsonify({"lokasi": global_lokasi_cache})

@app.route('/api/nearest-location', methods=['GET'])
def nearest_location():
    try:
        user_lat = float(request.args.get('lat'))
        user_lon = float(request.args.get('lon'))

        nearest = None
        min_dist = float('inf')

        # Cari dari cache memori
        for loc in global_lokasi_cache:
            dist = math.sqrt((user_lat - loc['lat'])**2 + (user_lon - loc['lon'])**2)
            if dist < min_dist:
                min_dist = dist
                nearest = loc

        return jsonify({"lokasi_terdekat": nearest})
    except:
        return jsonify({"error": "Invalid request"}), 400

if __name__ == '__main__':
    print("✨ Wargabantuin Backend Running...")
    app.run(debug=True, port=5000, use_reloader=False)
