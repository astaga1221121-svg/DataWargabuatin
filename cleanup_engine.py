# /home/Bisakah/backendnie/cleanup_engine.py

import os
import time
import glob

def clean_old_json_files(directory, interval_seconds):
    """
    Menghapus semua file JSON di direktori yang ditentukan secara berkala.
    
    Args:
        directory (str): Path absolut ke direktori yang akan dibersihkan.
        interval_seconds (int): Waktu jeda dalam detik antara setiap pembersihan.
    """
    print(f"🧹 Memulai tugas pembersihan file JSON di direktori: {directory}")
    while True:
        try:
            # Menggunakan glob untuk menemukan semua file .json di direktori
            files_to_delete = glob.glob(os.path.join(directory, "*.json"))
            
            if files_to_delete:
                print(f"🗑️ Menghapus {len(files_to_delete)} file JSON dari '{directory}'...")
                for file_path in files_to_delete:
                    os.remove(file_path)
                    print(f"✅ Terhapus: {file_path}")
            else:
                print(f"✅ Tidak ada file JSON untuk dihapus di '{directory}'.")
        except Exception as e:
            print(f"❌ Error saat membersihkan file: {e}")
        
        # Jeda selama interval yang ditentukan sebelum membersihkan lagi
        time.sleep(interval_seconds)

if __name__ == '__main__':
    # Contoh penggunaan untuk pengujian
    # Direktori harus diganti dengan path yang sesuai
    # Interval 2 menit = 120 detik
    directory_path = "/home/Bisakah/backendnie/sampahku"
    interval = 120
    clean_old_json_files(directory_path, interval)