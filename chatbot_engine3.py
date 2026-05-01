class QueryEngine:
    def __init__(self, data_engine):
        self.data_engine = data_engine
    
    def process_query(self, intent, matched_entities):
        """Process query berdasarkan intent"""
        
        # 1-2: Cuaca di lokasi
        if intent == "cuaca_lokasi":
            return self.get_cuaca_lokasi(matched_entities["lokasi"][0])
        
        # 3: Suhu tertinggi
        elif intent == "suhu_tertinggi":
            return self.get_suhu_tertinggi(matched_entities["lokasi"][0])
        
        # 4: Suhu terendah
        elif intent == "suhu_terendah":
            return self.get_suhu_terendah(matched_entities["lokasi"][0])
        
        # 5: Kelembapan
        elif intent == "kelembapan":
            return self.get_kelembapan(matched_entities["lokasi"][0])
        
        # 6-7: Ringkasan cuaca
        elif intent == "ringkasan_cuaca":
            return self.get_ringkasan_cuaca(matched_entities["lokasi"][0])
        
        # 8: Lokasi detail
        elif intent == "lokasi_detail":
            return self.get_lokasi_detail(matched_entities["lokasi"][0])
        
        # 9: Koordinat
        elif intent == "koordinat":
            return self.get_koordinat(matched_entities["lokasi"][0])
        
        # 10: List hewan
        elif intent == "list_hewan":
            return self.get_list_hewan()
        
        # 11: List sayuran
        elif intent == "list_sayuran":
            return self.get_list_sayuran()
        
        # 12: Kesesuaian hewan hari ini
        elif intent == "kesesuaian_hewan_hari_ini":
            return self.check_kesesuaian_hewan(matched_entities["hewan"][0], matched_entities["lokasi"][0])
        
        # Perbandingan
        elif intent == "perbandingan":
            return self.compare_locations(matched_entities["hewan"][0], matched_entities["lokasi"])
    
    def get_cuaca_lokasi(self, lokasi):
        """1-2: Ambil cuaca saat ini"""
        if lokasi["type"] == "desa":
            cuaca = lokasi["data"]["cuaca_saat_ini"]["cuaca"]
            ikon = lokasi["data"]["cuaca_saat_ini"].get("ikon", "")
            return {
                "cuaca": cuaca,
                "ikon": ikon,
                "lokasi": lokasi["name"]
            }
        else:
            # Agregasi untuk level yang lebih tinggi
            return self.aggregate_cuaca(lokasi["data"])
    
    def get_suhu_tertinggi(self, lokasi):
        """3: Suhu tertinggi hari ini"""
        if lokasi["type"] == "desa":
            return {
                "t_max": lokasi["data"]["ringkasan_harian"]["t_max"],
                "lokasi": lokasi["name"]
            }
        else:
            # Agregasi
            return self.aggregate_suhu_max(lokasi["data"])
    
    def get_suhu_terendah(self, lokasi):
        """4: Suhu terendah hari ini"""
        if lokasi["type"] == "desa":
            return {
                "t_min": lokasi["data"]["ringkasan_harian"]["t_min"],
                "lokasi": lokasi["name"]
            }
        else:
            return self.aggregate_suhu_min(lokasi["data"])
    
    def get_kelembapan(self, lokasi):
        """5: Kelembapan saat ini"""
        if lokasi["type"] == "desa":
            return {
                "kelembapan": lokasi["data"]["cuaca_saat_ini"]["kelembapan"],
                "lokasi": lokasi["name"]
            }
        else:
            return self.aggregate_kelembapan(lokasi["data"])
    
    def get_ringkasan_cuaca(self, lokasi):
        """6-7: Ringkasan cuaca harian"""
        if lokasi["type"] == "desa":
            ringkasan = lokasi["data"]["ringkasan_harian"]
            return {
                "t_max": ringkasan["t_max"],
                "t_min": ringkasan["t_min"],
                "t_avg": ringkasan["t_avg"],
                "hu_avg": ringkasan["hu_avg"],
                "cuaca_dominan": ringkasan["cuaca_dominan"],
                "lokasi": lokasi["name"]
            }
        else:
            return self.aggregate_ringkasan(lokasi["data"])
    
    def get_lokasi_detail(self, lokasi):
        """8: Detail lokasi"""
        data = lokasi["data"] if lokasi["type"] == "desa" else lokasi["data"][0]
        return {
            "provinsi": data["provinsi"],
            "kotkab": data["kotkab"],
            "kecamatan": data["kecamatan"],
            "desa": data["desa"]
        }
    
    def get_koordinat(self, lokasi):
        """9: Koordinat lokasi"""
        data = lokasi["data"] if lokasi["type"] == "desa" else lokasi["data"][0]
        return {
            "lat": data["lat"],
            "lon": data["lon"],
            "lokasi": lokasi["name"]
        }
    
    def get_list_hewan(self):
        """10: List semua hewan"""
        return {
            "hewan_list": list(self.data_engine.data_storage["hewan"]["all_names"])
        }
    
    def get_list_sayuran(self):
        """11: List semua sayuran"""
        return {
            "sayuran_list": list(self.data_engine.data_storage["sayuran"]["all_names"])
        }
    
    def check_kesesuaian_hewan(self, hewan, lokasi):
        """12: Check kesesuaian hewan"""
        hewan_data = hewan["data"]
        
        if lokasi["type"] == "desa":
            cuaca = lokasi["data"]["cuaca_saat_ini"]
            suhu = cuaca["suhu"]
            kelembapan = cuaca["kelembapan"]
        else:
            # Agregasi cuaca untuk level yang lebih tinggi
            aggregated = self.aggregate_cuaca(lokasi["data"])
            suhu = aggregated["suhu"]
            kelembapan = aggregated["kelembapan"]
        
        suhu_cocok = hewan_data["suhu_min"] <= suhu <= hewan_data["suhu_max"]
        kelembapan_cocok = hewan_data["hu_min"] <= kelembapan <= hewan_data["hu_max"]
        
        return {
            "cocok": suhu_cocok and kelembapan_cocok,
            "hewan": hewan["name"],
            "lokasi": lokasi["name"],
            "suhu_saat_ini": suhu,
            "kelembapan_saat_ini": kelembapan,
            "suhu_optimal": f"{hewan_data['suhu_min']}-{hewan_data['suhu_max']}°C",
            "kelembapan_optimal": f"{hewan_data['hu_min']}-{hewan_data['hu_max']}%"
        }
    
    def compare_locations(self, hewan, lokasi_list):
        """Perbandingan lokasi untuk hewan"""
        results = []
        
        for lokasi in lokasi_list:
            compatibility = self.check_kesesuaian_hewan(hewan, lokasi)
            score = self.calculate_compatibility_score(compatibility)
            results.append({
                "lokasi": lokasi["name"],
                "compatibility": compatibility,
                "score": score
            })
        
        # Sort berdasarkan score
        results.sort(key=lambda x: x["score"], reverse=True)
        
        return {
            "comparison": results,
            "winner": results[0]["lokasi"],
            "hewan": hewan["name"]
        }
    
    def calculate_compatibility_score(self, compatibility):
        """Hitung score kompatibilitas 0-100"""
        if compatibility["cocok"]:
            return 100
        
        # Hitung penalty berdasarkan jarak dari range optimal
        suhu_penalty = 0
        kelembapan_penalty = 0
        
        # Score berdasarkan seberapa jauh dari range optimal
        return max(0, 100 - suhu_penalty - kelembapan_penalty)
    
    def aggregate_cuaca(self, desa_list):
        """Agregasi cuaca untuk multiple desa"""
        if not isinstance(desa_list, list):
            desa_list = [desa_list]
        
        total_suhu = sum(desa["cuaca_saat_ini"]["suhu"] for desa in desa_list)
        total_kelembapan = sum(desa["cuaca_saat_ini"]["kelembapan"] for desa in desa_list)
        
        cuaca_count = {}
        for desa in desa_list:
            cuaca = desa["cuaca_saat_ini"]["cuaca"]
            cuaca_count[cuaca] = cuaca_count.get(cuaca, 0) + 1
        
        count = len(desa_list)
        return {
            "suhu": round(total_suhu / count, 1),
            "kelembapan": round(total_kelembapan / count, 1),
            "cuaca": max(cuaca_count, key=cuaca_count.get)
        }