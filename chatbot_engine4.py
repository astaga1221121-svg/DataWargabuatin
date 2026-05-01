class ResponseEngine:
    def __init__(self):
        self.response_templates = self.build_templates()
    
    def build_templates(self):
        """Template response untuk setiap jenis pertanyaan"""
        return {
            "cuaca_lokasi": "Cuaca di {lokasi} saat ini adalah {cuaca}.",
            "suhu_tertinggi": "Suhu tertinggi hari ini di {lokasi} adalah {t_max}°C.",
            "suhu_terendah": "Suhu terendah hari ini di {lokasi} adalah {t_min}°C.",
            "kelembapan": "Kelembapan saat ini di {lokasi} adalah {kelembapan}%.",
            "ringkasan_cuaca": "Ringkasan cuaca hari ini di {lokasi}: suhu tertinggi {t_max}°C, terendah {t_min}°C, rata-rata {t_avg}°C, kelembapan rata-rata {hu_avg}%, cuaca dominan {cuaca_dominan}.",
            "lokasi_detail": "{desa} terletak di Kecamatan {kecamatan}, {kotkab}, Provinsi {provinsi}.",
            "koordinat": "Koordinat {lokasi} adalah (lat: {lat}, lon: {lon}).",
            "list_hewan": "Hewan yang dapat dicek: {hewan_list}.",
            "list_sayuran": "Sayuran yang tersedia: {sayuran_list}.",
            "kesesuaian_hewan": "{hasil_kesesuaian}",
            "perbandingan": "{hasil_perbandingan}"
        }
    
    def generate_response(self, intent, query_result):
        """Generate response berdasarkan intent dan hasil query"""
        
        # 1-2: Cuaca lokasi
        if intent == "cuaca_lokasi":
            return self.response_templates["cuaca_lokasi"].format(
                lokasi=query_result["lokasi"],
                cuaca=query_result["cuaca"]
            )
        
        # 3: Suhu tertinggi
        elif intent == "suhu_tertinggi":
            return self.response_templates["suhu_tertinggi"].format(
                lokasi=query_result["lokasi"],
                t_max=query_result["t_max"]
            )
        
        # 4: Suhu terendah
        elif intent == "suhu_terendah":
            return self.response_templates["suhu_terendah"].format(
                lokasi=query_result["lokasi"],
                t_min=query_result["t_min"]
            )
        
        # 5: Kelembapan
        elif intent == "kelembapan":
            return self.response_templates["kelembapan"].format(
                lokasi=query_result["lokasi"],
                kelembapan=query_result["kelembapan"]
            )
        
        # 6-7: Ringkasan cuaca
        elif intent == "ringkasan_cuaca":
            return self.response_templates["ringkasan_cuaca"].format(**query_result)
        
        # 8: Lokasi detail
        elif intent == "lokasi_detail":
            return self.response_templates["lokasi_detail"].format(**query_result)
        
        # 9: Koordinat
        elif intent == "koordinat":
            return self.response_templates["koordinat"].format(**query_result)
        
        # 10: List hewan
        elif intent == "list_hewan":
            hewan_str = ", ".join(query_result["hewan_list"])
            return self.response_templates["list_hewan"].format(hewan_list=hewan_str)
        
        # 11: List sayuran
        elif intent == "list_sayuran":
            sayuran_str = ", ".join(query_result["sayuran_list"])
            return self.response_templates["list_sayuran"].format(sayuran_list=sayuran_str)
        
        # 12: Kesesuaian hewan
        elif intent == "kesesuaian_hewan_hari_ini":
            return self.generate_kesesuaian_response(query_result)
        
        # Perbandingan
        elif intent == "perbandingan":
            return self.generate_perbandingan_response(query_result)
        
        # Default response
        else:
            return "Maaf, saya tidak dapat memahami pertanyaan Anda. Silakan coba lagi."
    
    def generate_kesesuaian_response(self, result):
        """Generate response untuk kesesuaian hewan"""
        if result["cocok"]:
            response = f"✅ {result['hewan']} cocok dipelihara di {result['lokasi']} hari ini.\n\n"
        else:
            response = f"❌ {result['hewan']} tidak cocok dipelihara di {result['lokasi']} hari ini.\n\n"
        
        response += f"**Kondisi Saat Ini:**\n"
        response += f"• Suhu: {result['suhu_saat_ini']}°C (optimal: {result['suhu_optimal']})\n"
        response += f"• Kelembapan: {result['kelembapan_saat_ini']}% (optimal: {result['kelembapan_optimal']})"
        
        return response
    
    def generate_perbandingan_response(self, result):
        """Generate response untuk perbandingan lokasi"""
        response = f"**Perbandingan lokasi terbaik untuk memelihara {result['hewan']}:**\n\n"
        response += f"🏆 **Pemenang: {result['winner']}**\n\n"
        
        for i, comparison in enumerate(result['comparison'], 1):
            lokasi = comparison['lokasi']
            comp = comparison['compatibility']
            score = comparison['score']
            
            status = "✅ Cocok" if comp['cocok'] else "❌ Tidak Cocok"
            response += f"{i}. **{lokasi}** - {status} (Score: {score:.1f}%)\n"
            response += f"   • Suhu: {comp['suhu_saat_ini']}°C | Kelembapan: {comp['kelembapan_saat_ini']}%\n\n"
        
        return response


# ============================================================================
# IMPLEMENTASI LENGKAP UNTUK SEMUA 43 JENIS PERTANYAAN
# ============================================================================

class ComprehensiveQueryEngine(QueryEngine):
    """Extended Query Engine untuk semua 43 pertanyaan"""
    
    def process_all_queries(self, intent, matched_entities, user_input):
        """Process semua jenis query 1-43"""
        
        # 1-12: Sudah diimplementasi di atas
        if intent in ["cuaca_lokasi", "suhu_tertinggi", "suhu_terendah", "kelembapan", 
                     "ringkasan_cuaca", "lokasi_detail", "koordinat", "list_hewan", 
                     "list_sayuran", "kesesuaian_hewan_hari_ini", "perbandingan"]:
            return self.process_query(intent, matched_entities)
        
        # 13: Hewan cocok di provinsi
        elif intent == "hewan_cocok_provinsi":
            return self.get_hewan_cocok_provinsi(matched_entities["lokasi"][0])
        
        # 14: Sayuran cocok di desa
        elif intent == "sayuran_cocok_desa":
            return self.get_sayuran_cocok_desa(matched_entities["lokasi"][0])
        
        # 15: Kecamatan cocok untuk hewan/sayuran
        elif intent == "kecamatan_cocok":
            return self.get_kecamatan_cocok(matched_entities)
        
        # 16: Suhu maksimum kotkab
        elif intent == "suhu_max_kotkab":
            return self.get_suhu_max_kotkab(matched_entities["lokasi"][0])
        
        # 17: Kelembapan rata-rata harian desa
        elif intent == "kelembapan_avg_desa":
            return self.get_kelembapan_avg_desa(matched_entities["lokasi"][0])
        
        # 18: Cuaca dominan provinsi
        elif intent == "cuaca_dominan_provinsi":
            return self.get_cuaca_dominan_provinsi(matched_entities["lokasi"][0])
        
        # 19: Desa cuaca cerah di provinsi
        elif intent == "desa_cerah_provinsi":
            return self.get_desa_cerah_provinsi(matched_entities["lokasi"][0])
        
        # 20: Provinsi cocok untuk sayuran dengan suhu min
        elif intent == "provinsi_cocok_sayuran_suhu_min":
            return self.get_provinsi_cocok_sayuran_suhu_min(matched_entities)
        
        # 21: Hewan tidak cocok karena suhu tinggi
        elif intent == "hewan_tidak_cocok_suhu_tinggi":
            return self.get_hewan_tidak_cocok_suhu_tinggi(matched_entities["lokasi"][0])
        
        # 22: Sayuran tidak cocok karena kelembapan rendah
        elif intent == "sayuran_tidak_cocok_kelembapan_rendah":
            return self.get_sayuran_tidak_cocok_kelembapan_rendah(matched_entities["lokasi"][0])
        
        # 23: Suhu rata-rata kabupaten
        elif intent == "suhu_avg_kabupaten":
            return self.get_suhu_avg_kabupaten(matched_entities["lokasi"][0])
        
        # 24: Suhu tertinggi dan terendah desa
        elif intent == "suhu_tertinggi_terendah_desa":
            return self.get_suhu_tertinggi_terendah_desa(matched_entities["lokasi"][0])
        
        # 25: Ramalan cuaca harian (sama dengan ringkasan)
        elif intent == "ramalan_cuaca_harian":
            return self.get_ringkasan_cuaca(matched_entities["lokasi"][0])
        
        # 26: Cuaca saat ini desa (sama dengan cuaca lokasi)
        elif intent == "cuaca_saat_ini_desa":
            return self.get_cuaca_lokasi(matched_entities["lokasi"][0])
        
        # 27: Suhu dan kelembapan saat ini
        elif intent == "suhu_kelembapan_saat_ini":
            return self.get_suhu_kelembapan_saat_ini(matched_entities["lokasi"][0])
        
        # 28: Lokasi sesuai kelembapan hewan
        elif intent == "lokasi_sesuai_kelembapan_hewan":
            return self.get_lokasi_sesuai_kelembapan_hewan(matched_entities["hewan"][0])
        
        # 29: Lokasi sesuai suhu sayuran
        elif intent == "lokasi_sesuai_suhu_sayuran":
            return self.get_lokasi_sesuai_suhu_sayuran(matched_entities["sayuran"][0])
        
        # 30: Desa suhu mendekati ideal untuk hewan
        elif intent == "desa_suhu_ideal_hewan":
            return self.get_desa_suhu_ideal_hewan(matched_entities)
        
        # 31: Kecamatan cocok untuk sayuran
        elif intent == "kecamatan_cocok_sayuran":
            return self.get_kecamatan_cocok_sayuran(matched_entities)
        
        # 32: Cuaca dominan provinsi (sama dengan 18)
        elif intent == "cuaca_dominan_provinsi_2":
            return self.get_cuaca_dominan_provinsi(matched_entities["lokasi"][0])
        
        # 33: Suhu cocok untuk hewan di desa
        elif intent == "suhu_cocok_hewan_desa":
            return self.check_suhu_cocok_hewan_desa(matched_entities)
        
        # 34: Kabupaten kelembapan tinggi untuk sayuran
        elif intent == "kabupaten_kelembapan_tinggi_sayuran":
            return self.get_kabupaten_kelembapan_tinggi_sayuran(matched_entities["sayuran"][0])
        
        # 35: Kecamatan cuaca mendung
        elif intent == "kecamatan_cuaca_mendung":
            return self.get_kecamatan_cuaca_mendung()
        
        # 36: Provinsi suhu min untuk sayuran
        elif intent == "provinsi_suhu_min_sayuran":
            return self.get_provinsi_suhu_min_sayuran(matched_entities["sayuran"][0])
        
        # 37: Desa suhu paling rendah di provinsi
        elif intent == "desa_suhu_rendah_provinsi":
            return self.get_desa_suhu_rendah_provinsi(matched_entities["lokasi"][0])
        
        # 38: Desa suhu maksimum paling tinggi
        elif intent == "desa_suhu_max_tinggi":
            return self.get_desa_suhu_max_tinggi()
        
        # 39: Kabupaten kelembapan rata-rata paling rendah
        elif intent == "kabupaten_kelembapan_rendah":
            return self.get_kabupaten_kelembapan_rendah()
        
        # 40: Desa suhu mendekati ideal untuk hewan (sama dengan 30)
        elif intent == "desa_suhu_ideal_hewan_2":
            return self.get_desa_suhu_ideal_hewan(matched_entities)
        
        # 41: Suhu max, min, avg, kelembapan avg desa
        elif intent == "detail_lengkap_desa":
            return self.get_detail_lengkap_desa(matched_entities["lokasi"][0])
        
        # 42: Lokasi tidak cocok untuk hewan
        elif intent == "lokasi_tidak_cocok_hewan":
            return self.get_lokasi_tidak_cocok_hewan(matched_entities["hewan"][0])
        
        # 43: Provinsi cocok untuk sayuran
        elif intent == "provinsi_cocok_sayuran":
            return self.get_provinsi_cocok_sayuran(matched_entities["sayuran"][0])
    
    # ========================================================================
    # IMPLEMENTASI METODE UNTUK PERTANYAAN 13-43
    # ========================================================================
    
    def get_hewan_cocok_provinsi(self, provinsi):
        """13: Hewan yang cocok di provinsi berdasarkan rata-rata"""
        desa_list = provinsi["data"]
        
        # Hitung rata-rata suhu dan kelembapan provinsi
        total_t_avg = sum(desa["ringkasan_harian"]["t_avg"] for desa in desa_list)
        total_hu_avg = sum(desa["ringkasan_harian"]["hu_avg"] for desa in desa_list)
        
        avg_suhu = total_t_avg / len(desa_list)
        avg_kelembapan = total_hu_avg / len(desa_list)
        
        # Filter hewan yang cocok
        hewan_cocok = []
        for nama_hewan, data_hewan in self.data_engine.data_storage["hewan"]["by_nama"].items():
            if (data_hewan["suhu_min"] <= avg_suhu <= data_hewan["suhu_max"] and
                data_hewan["hu_min"] <= avg_kelembapan <= data_hewan["hu_max"]):
                hewan_cocok.append(nama_hewan)
        
        return {
            "provinsi": provinsi["name"],
            "avg_suhu": round(avg_suhu, 1),
            "avg_kelembapan": round(avg_kelembapan, 1),
            "hewan_cocok": hewan_cocok
        }
    
    def get_sayuran_cocok_desa(self, desa):
        """14: Sayuran yang cocok di desa dengan kondisi saat ini"""
        cuaca = desa["data"]["cuaca_saat_ini"]
        suhu = cuaca["suhu"]
        kelembapan = cuaca["kelembapan"]
        
        sayuran_cocok = []
        for nama_sayuran, data_sayuran in self.data_engine.data_storage["sayuran"]["by_nama"].items():
            if (data_sayuran["suhu_min"] <= suhu <= data_sayuran["suhu_max"] and
                data_sayuran["hu_min"] <= kelembapan <= data_sayuran["hu_max"]):
                sayuran_cocok.append(nama_sayuran)
        
        return {
            "desa": desa["name"],
            "suhu": suhu,
            "kelembapan": kelembapan,
            "sayuran_cocok": sayuran_cocok
        }
    
    def get_kecamatan_cocok(self, matched_entities):
        """15: Kecamatan yang cocok untuk beternak X atau menanam Y"""
        # Extract hewan dan sayuran dari entities
        target_hewan = matched_entities["hewan"][0]["name"] if matched_entities["hewan"] else None
        target_sayuran = matched_entities["sayuran"][0]["name"] if matched_entities["sayuran"] else None
        kabupaten = matched_entities["lokasi"][0]
        
        kecamatan_cocok = []
        
        # Loop semua kecamatan di kabupaten
        for kecamatan_name, desa_list in self.data_engine.data_storage["lokasi"]["by_kecamatan"].items():
            # Filter desa yang ada di kabupaten target
            desa_di_kabupaten = [desa for desa in desa_list if desa["kotkab"].lower() == kabupaten["name"]]
            
            if not desa_di_kabupaten:
                continue
            
            # Hitung rata-rata kecamatan
            total_t_avg = sum(desa["ringkasan_harian"]["t_avg"] for desa in desa_di_kabupaten)
            total_hu_avg = sum(desa["ringkasan_harian"]["hu_avg"] for desa in desa_di_kabupaten)
            
            avg_suhu = total_t_avg / len(desa_di_kabupaten)
            avg_kelembapan = total_hu_avg / len(desa_di_kabupaten)
            
            # Check kesesuaian
            cocok_hewan = False
            cocok_sayuran = False
            
            if target_hewan:
                hewan_data = self.data_engine.data_storage["hewan"]["by_nama"].get(target_hewan)
                if hewan_data:
                    cocok_hewan = (hewan_data["suhu_min"] <= avg_suhu <= hewan_data["suhu_max"] and
                                  hewan_data["hu_min"] <= avg_kelembapan <= hewan_data["hu_max"])
            
            if target_sayuran:
                sayuran_data = self.data_engine.data_storage["sayuran"]["by_nama"].get(target_sayuran)
                if sayuran_data:
                    cocok_sayuran = (sayuran_data["suhu_min"] <= avg_suhu <= sayuran_data["suhu_max"] and
                                   sayuran_data["hu_min"] <= avg_kelembapan <= sayuran_data["hu_max"])
            
            if cocok_hewan or cocok_sayuran:
                kecamatan_cocok.append({
                    "kecamatan": kecamatan_name,
                    "avg_suhu": round(avg_suhu, 1),
                    "avg_kelembapan": round(avg_kelembapan, 1),
                    "cocok_hewan": cocok_hewan,
                    "cocok_sayuran": cocok_sayuran
                })
        
        return {
            "kabupaten": kabupaten["name"],
            "target_hewan": target_hewan,
            "target_sayuran": target_sayuran,
            "kecamatan_cocok": kecamatan_cocok
        }
    
    def get_suhu_max_kotkab(self, kotkab):
        """16: Suhu maksimum di kotkab"""
        desa_list = kotkab["data"]
        max_suhu = max(desa["ringkasan_harian"]["t_max"] for desa in desa_list)
        
        return {
            "kotkab": kotkab["name"],
            "t_max": max_suhu
        }
    
    def get_kelembapan_avg_desa(self, desa):
        """17: Kelembapan rata-rata harian di desa"""
        hu_avg = desa["data"]["ringkasan_harian"]["hu_avg"]
        
        return {
            "desa": desa["name"],
            "hu_avg": hu_avg
        }
    
    def get_cuaca_dominan_provinsi(self, provinsi):
        """18: Cuaca dominan hari ini di provinsi"""
        desa_list = provinsi["data"]
        cuaca_count = {}
        
        for desa in desa_list:
            cuaca = desa["ringkasan_harian"]["cuaca_dominan"]
            cuaca_count[cuaca] = cuaca_count.get(cuaca, 0) + 1
        
        cuaca_dominan = max(cuaca_count, key=cuaca_count.get)
        
        return {
            "provinsi": provinsi["name"],
            "cuaca_dominan": cuaca_dominan,
            "distribusi": cuaca_count
        }
    
    def get_desa_cerah_provinsi(self, provinsi):
        """19: Desa yang cuacanya cerah di provinsi"""
        desa_list = provinsi["data"]
        desa_cerah = []
        
        for desa in desa_list:
            if "cerah" in desa["ringkasan_harian"]["cuaca_dominan"].lower():
                desa_cerah.append(desa["desa"])
        
        return {
            "provinsi": provinsi["name"],
            "desa_cerah": desa_cerah
        }
    
    def get_suhu_kelembapan_saat_ini(self, lokasi):
        """27: Suhu dan kelembapan saat ini"""
        if lokasi["type"] == "desa":
            cuaca = lokasi["data"]["cuaca_saat_ini"]
            return {
                "lokasi": lokasi["name"],
                "suhu": cuaca["suhu"],
                "kelembapan": cuaca["kelembapan"]
            }
        else:
            # Agregasi untuk level yang lebih tinggi
            aggregated = self.aggregate_cuaca(lokasi["data"])
            return {
                "lokasi": lokasi["name"],
                "suhu": aggregated["suhu"],
                "kelembapan": aggregated["kelembapan"]
            }
    
    def get_detail_lengkap_desa(self, desa):
        """41: Detail lengkap suhu dan kelembapan desa"""
        ringkasan = desa["data"]["ringkasan_harian"]
        
        return {
            "desa": desa["name"],
            "t_max": ringkasan["t_max"],
            "t_min": ringkasan["t_min"],
            "t_avg": ringkasan["t_avg"],
            "hu_avg": ringkasan["hu_avg"]
        }


# ============================================================================
# EXTENDED RESPONSE ENGINE UNTUK SEMUA 43 PERTANYAAN
# ============================================================================

class ComprehensiveResponseEngine(ResponseEngine):
    """Extended Response Engine untuk semua 43 pertanyaan"""
    
    def generate_comprehensive_response(self, intent, query_result):
        """Generate response untuk semua 43 jenis pertanyaan"""
        
        # 13: Hewan cocok provinsi
        if intent == "hewan_cocok_provinsi":
            if query_result["hewan_cocok"]:
                hewan_list = ", ".join(query_result["hewan_cocok"])
                return f"Hewan yang cocok diternak di Provinsi {query_result['provinsi']} berdasarkan kondisi rata-rata (suhu: {query_result['avg_suhu']}°C, kelembapan: {query_result['avg_kelembapan']}%) adalah: {hewan_list}."
            else:
                return f"Tidak ada hewan yang cocok diternak di Provinsi {query_result['provinsi']} dengan kondisi rata-rata saat ini."
        
        # 14: Sayuran cocok desa
        elif intent == "sayuran_cocok_desa":
            if query_result["sayuran_cocok"]:
                sayuran_list = ", ".join(query_result["sayuran_cocok"])
                return f"Sayuran yang cocok ditanam di {query_result['desa']} dengan kondisi saat ini (suhu: {query_result['suhu']}°C, kelembapan: {query_result['kelembapan']}%) adalah: {sayuran_list}."
            else:
                return f"Tidak ada sayuran yang cocok ditanam di {query_result['desa']} dengan kondisi saat ini."
        
        # 15: Kecamatan cocok
        elif intent == "kecamatan_cocok":
            if query_result["kecamatan_cocok"]:
                response = f"Kecamatan di {query_result['kabupaten']} yang cocok:\n\n"
                for kec in query_result["kecamatan_cocok"]:
                    status = []
                    if kec["cocok_hewan"]:
                        status.append(f"beternak {query_result['target_hewan']}")
                    if kec["cocok_sayuran"]:
                        status.append(f"menanam {query_result['target_sayuran']}")
                    
                    response += f"• **{kec['kecamatan']}** - Cocok untuk {' dan '.join(status)}\n"
                    response += f"  Suhu rata-rata: {kec['avg_suhu']}°C, Kelembapan: {kec['avg_kelembapan']}%\n\n"
                return response
            else:
                return f"Tidak ada kecamatan di {query_result['kabupaten']} yang cocok untuk kriteria tersebut."
        
        # 16: Suhu maksimum kotkab
        elif intent == "suhu_max_kotkab":
            return f"Suhu maksimum hari ini di {query_result['kotkab']} adalah {query_result['t_max']}°C."
        
        # 17: Kelembapan rata-rata desa
        elif intent == "kelembapan_avg_desa":
            return f"Kelembapan rata-rata harian di {query_result['desa']} adalah {query_result['hu_avg']}%."
        
        # 18: Cuaca dominan provinsi
        elif intent == "cuaca_dominan_provinsi":
            return f"Cuaca dominan hari ini di Provinsi {query_result['provinsi']} adalah {query_result['cuaca_dominan']}."
        
        # 19: Desa cerah di provinsi
        elif intent == "desa_cerah_provinsi":
            if query_result["desa_cerah"]:
                desa_list = ", ".join(query_result["desa_cerah"])
                return f"Desa di Provinsi {query_result['provinsi']} yang cuacanya cerah hari ini: {desa_list}."
            else:
                return f"Tidak ada desa di Provinsi {query_result['provinsi']} yang cuacanya cerah hari ini."
        
        # 27: Suhu dan kelembapan saat ini
        elif intent == "suhu_kelembapan_saat_ini":
            return f"Suhu dan kelembapan saat ini di {query_result['lokasi']}: {query_result['suhu']}°C, {query_result['kelembapan']}%."
        
        # 41: Detail lengkap desa
        elif intent == "detail_lengkap_desa":
            return f"Detail cuaca {query_result['desa']} hari ini: suhu maksimum {query_result['t_max']}°C, minimum {query_result['t_min']}°C, rata-rata {query_result['t_avg']}°C, kelembapan rata-rata {query_result['hu_avg']}%."
        
        # Default untuk intent lainnya
        else:
            return self.generate_response(intent, query_result)


# ============================================================================
# MAIN CHATBOT CLASS - KOORDINASI SEMUA ENGINE
# ============================================================================

class SmartCareMapChatbot:
    """Main chatbot class yang mengkoordinasikan semua engine"""
    
    def __init__(self):
        self.data_engine = DataEngine()
        self.nlp_engine = None
        self.query_engine = None
        self.response_engine = None
        self.is_initialized = False
    
    def initialize(self):
        """Initialize semua engine"""
        print("Memuat data...")
        self.data_engine.load_all_data()
        
        self.nlp_engine = NLPEngine(self.data_engine)
        self.query_engine = ComprehensiveQueryEngine(self.data_engine)
        self.response_engine = ComprehensiveResponseEngine()
        
        self.is_initialized = True
        print("Chatbot siap digunakan!")
    
    def process_query(self, user_input):
        """Proses query user dari awal hingga akhir"""
        if not self.is_initialized:
            self.initialize()
        
        try:
            # Step 1: NLP Processing
            intent_result = self.nlp_engine.classify_intent(user_input)
            
            if intent_result["intent"] == "unknown":
                return "Maaf, saya tidak dapat memahami pertanyaan Anda. Silakan coba dengan format yang lebih jelas."
            
            # Step 2: Entity Extraction & Matching
            entities = self.nlp_engine.extract_entities(user_input, intent_result)
            matched_entities = self.match_all_entities(entities)
            
            # Step 3: Query Processing
            if intent_result["intent"] in ["list_hewan", "list_sayuran"]:
                query_result = self.query_engine.process_query(intent_result["intent"], matched_entities)
            else:
                if not self.validate_entities(matched_entities, intent_result["intent"]):
                    return self.generate_entity_error_message(entities, intent_result["intent"])
                
                query_result = self.query_engine.process_all_queries(
                    intent_result["intent"], matched_entities, user_input
                )
            
            # Step 4: Response Generation
            response = self.response_engine.generate_comprehensive_response(
                intent_result["intent"], query_result
            )
            
            return response
            
        except Exception as e:
            return f"Terjadi kesalahan dalam memproses pertanyaan Anda. Silakan coba lagi. Error: {str(e)}"
    
    def match_all_entities(self, entities):
        """Match semua entities dengan fuzzy matching"""
        matched = {
            "lokasi": [],
            "hewan": [],
            "sayuran": []
        }
        
        # Match lokasi
        for lokasi_raw in entities["lokasi"]:
            matched_lokasi = self.nlp_engine.fuzzy_match_lokasi(lokasi_raw)
            if matched_lokasi:
                matched["lokasi"].append(matched_lokasi)
        
        # Match hewan
        for hewan_raw in entities["hewan"]:
            matched_hewan = self.nlp_engine.fuzzy_match_hewan(hewan_raw)
            if matched_hewan:
                matched["hewan"].append(matched_hewan)
        
        # Match sayuran
        for sayuran_raw in entities["sayuran"]:
            matched_sayuran = self.nlp_engine.fuzzy_match_sayuran(sayuran_raw)
            if matched_sayuran:
                matched["sayuran"].append(matched_sayuran)
        
        return matched
    
    def validate_entities(self, matched_entities, intent):
        """Validasi apakah entities yang dibutuhkan tersedia"""
        requirements = {
            "cuaca_lokasi": ["lokasi"],
            "suhu_tertinggi": ["lokasi"],
            "perbandingan": ["lokasi", "hewan"],
            "kesesuaian_hewan_hari_ini": ["hewan"],
            # Add more requirements as needed
        }
        
        required = requirements.get(intent, [])
        
        for req in required:
            if not matched_entities[req]:
                return False
        
        return True
    
    def generate_entity_error_message(self, entities, intent):
        """Generate error message untuk entity yang tidak ditemukan"""
        if not entities["lokasi"] and intent in ["cuaca_lokasi", "suhu_tertinggi"]:
            return "Lokasi yang Anda sebutkan tidak ditemukan. Silakan sebutkan nama desa, kecamatan, kabupaten, atau provinsi yang valid."
        
        if not entities["hewan"] and intent in ["kesesuaian_hewan_hari_ini", "perbandingan"]:
            available_hewan = list(self.data_engine.data_storage["hewan"]["all_names"])[:10]
            return f"Hewan yang Anda sebutkan tidak ditemukan. Contoh hewan yang tersedia: {', '.join(available_hewan)}."
        
        return "Ada informasi yang kurang dalam pertanyaan Anda. Silakan lengkapi dan coba lagi."


# ============================================================================
# PENGGUNAAN CHATBOT
# ============================================================================

def main():
    """Contoh penggunaan chatbot"""
    
    chatbot = SmartCareMapChatbot()
    
    print("=== Smart Care Map Chatbot ===")
    print("Ketik 'exit' untuk keluar\n")
    
    while True:
        user_input = input("Anda: ").strip()