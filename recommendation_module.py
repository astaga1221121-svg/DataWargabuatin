# recommendation_module.py
import os
import json
import datetime
from collections import Counter
import pytz

# Import dari ai_engine
from ai_engine import DATA_DIR, CACHE_DIR, load_json

def skor_cocok_item(item, rt_suhu, rt_hu, avg_suhu, avg_hu):
    """Menghitung skor kecocokan 0-100."""
    s_min, s_max = item.get("suhu_min", 0), item.get("suhu_max", 40)
    h_min, h_max = item.get("hu_min", 0), item.get("hu_max", 100)

    skor = 100
    if rt_suhu is not None and not (s_min <= rt_suhu <= s_max): skor -= 30
    if rt_hu is not None and not (h_min <= rt_hu <= h_max): skor -= 30
    return max(0, skor), "Kondisi sesuai" if skor == 100 else "Kondisi kurang ideal"

def smart_rekomendasi(keyword):
    keyword = keyword.lower()
    hewan_list = load_json(os.path.join(DATA_DIR, 'hewan_cocok.json'))
    sayuran_list = load_json(os.path.join(DATA_DIR, 'sayuran_cocok.json'))
    results = []

    tz = pytz.timezone("Asia/Jakarta")
    now = datetime.datetime.now(tz=tz)
    today_str = now.strftime('%Y-%m-%d')

    if not os.path.exists(CACHE_DIR): return []

    # Mapping ikon cuaca
    weather_icons = {
        "hujan": "https://api-apps.bmkg.go.id/storage/icon/cuaca/hujan%20ringan-pm.svg",
        "berawan": "https://api-apps.bmkg.go.id/storage/icon/cuaca/berawan-am.svg",
        "cerah berawan": "https://api-apps.bmkg.go.id/storage/icon/cuaca/cerah%20berawan-am.svg",
        "cerah": "https://api-apps.bmkg.go.id/storage/icon/cuaca/cerah-am.svg",
        "default": "https://api-apps.bmkg.go.id/storage/icon/cuaca/berawan-am.svg"
    }

    for filename in os.listdir(CACHE_DIR):
        if not filename.endswith('.json'): continue

        try:
            with open(os.path.join(CACHE_DIR, filename), encoding='utf-8') as f:
                raw_payload = json.load(f)

            main_data = raw_payload.get('data', [])
            if not main_data: continue

            entry = main_data[0] if isinstance(main_data, list) else raw_payload
            lokasi = entry.get('lokasi', {})
            cuaca_list = entry.get('cuaca', [])

            flat_cuaca = []
            for sub in cuaca_list:
                if isinstance(sub, list): flat_cuaca.extend(sub)
                else: flat_cuaca.append(sub)

            try:
                lat = float(lokasi.get('lat'))
                lon = float(lokasi.get('lon'))
            except: continue

            closest = None
            min_diff = float('inf')
            t_vals, hu_vals = [], []
            t_today_vals = []

            for c in flat_cuaca:
                t, hu = c.get('t'), c.get('hu')
                dt_str = c.get('local_datetime') or c.get('datetime')

                if isinstance(t, (int, float)):
                    t_vals.append(t)
                    if dt_str and dt_str.startswith(today_str):
                        t_today_vals.append(t)
                if isinstance(hu, (int, float)): hu_vals.append(hu)

                if dt_str:
                    try:
                        c_dt = tz.localize(datetime.datetime.strptime(dt_str.split('.')[0].replace('T', ' '), "%Y-%m-%d %H:%M:%S"))
                        diff = abs((c_dt - now).total_seconds())
                        if diff < min_diff:
                            min_diff = diff
                            closest = c
                    except: pass

            t_rt = closest.get('t') if closest else None
            hu_rt = closest.get('hu') if closest else None
            desc = closest.get('weather_desc', 'Berawan')

            # Icon selection
            icon_url = weather_icons["default"]
            desc_l = desc.lower()
            if "hujan" in desc_l: icon_url = weather_icons["hujan"]
            elif "cerah berawan" in desc_l: icon_url = weather_icons["cerah berawan"]
            elif "cerah" in desc_l: icon_url = weather_icons["cerah"]
            elif "berawan" in desc_l: icon_url = weather_icons["berawan"]

            t_avg = round(sum(t_vals)/len(t_vals), 1) if t_vals else None
            hu_avg = round(sum(hu_vals)/len(hu_vals), 1) if hu_vals else None

            # Rekomendasi
            rec_h, rec_s = [], []
            pen_h, pen_s = [], []
            for h in hewan_list:
                skor, alasan = skor_cocok_item(h, t_rt, hu_rt, t_avg, hu_avg)
                if skor > 0:
                    pen_h.append({"nama": h['nama'], "skor": skor, "alasan_skor": alasan})
                    if skor >= 70: rec_h.append(h['nama'])
            for s in sayuran_list:
                skor, alasan = skor_cocok_item(s, t_rt, hu_rt, t_avg, hu_avg)
                if skor > 0:
                    pen_s.append({"nama": s['nama'], "skor": skor, "alasan_skor": alasan})
                    if skor >= 70: rec_s.append(s['nama'])

            # Filter keyword
            match_keyword = not keyword or any(keyword in str(lokasi.get(k,'')).lower() for k in ['desa','kecamatan','kotkab'])

            if match_keyword:
                results.append({
                    "adm4": lokasi.get('adm4', filename[:-5]),
                    "desa": lokasi.get('desa', 'N/A'),
                    "kecamatan": lokasi.get('kecamatan', 'N/A'),
                    "kotkab": lokasi.get('kotkab', 'N/A'),
                    "provinsi": lokasi.get('provinsi', 'N/A'),
                    "lat": lat, "lon": lon,
                    "suhu_realtime": t_rt,
                    "kelembapan_realtime": hu_rt,
                    "weather_desc": desc,
                    "weather_icon_url": icon_url,
                    "suhu_hari_ini": {
                        "rata2": round(sum(t_today_vals)/len(t_today_vals), 1) if t_today_vals else t_rt,
                        "max": max(t_today_vals) if t_today_vals else t_rt,
                        "min": min(t_today_vals) if t_today_vals else t_rt
                    },
                    "rata2_suhu": t_avg,
                    "rata2_hu": hu_avg,
                    "pilihan_tepat": {"hewan": rec_h, "sayuran": rec_s},
                    "cocok_untuk": {"hewan": pen_h, "sayuran": pen_s}
                })
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return results
