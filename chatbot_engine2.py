class NLPEngine:
    def __init__(self, data_engine):
        self.data_engine = data_engine
        self.intent_patterns = self.build_intent_patterns()
    
    def build_intent_patterns(self):
        """Build pattern untuk 43 jenis pertanyaan"""
        return {
            # 1-2: Cuaca di lokasi
            "cuaca_lokasi": [
                r"cuaca di (.+)",
                r"bagaimana cuaca di (.+)",
                r"cuaca saat ini di (.+)"
            ],
            # 3: Suhu tertinggi
            "suhu_tertinggi": [
                r"suhu tertinggi di (.+)",
                r"suhu maksimum di (.+)",
                r"suhu max.*di (.+)"
            ],
            # 4: Suhu terendah
            "suhu_terendah": [
                r"suhu terendah.*di (.+)",
                r"suhu minimum di (.+)",
                r"suhu min.*di (.+)"
            ],
            # 5: Kelembapan
            "kelembapan": [
                r"kelembapan di (.+)",
                r"kelembapan saat ini di (.+)"
            ],
            # 6-7: Ringkasan cuaca
            "ringkasan_cuaca": [
                r"ringkasan cuaca.*di (.+)",
                r"summary cuaca (.+)",
                r"ramalan cuaca.*di (.+)"
            ],
            # 8: Lokasi
            "lokasi_detail": [
                r"dimana letak (.+)",
                r"di mana (.+)"
            ],
            # 9: Koordinat
            "koordinat": [
                r"koordinat (.+)",
                r"posisi (.+)"
            ],
            # 10: List hewan
            "list_hewan": [
                r"apa saja hewan yang dapat dicek",
                r"daftar hewan"
            ],
            # 11: List sayuran
            "list_sayuran": [
                r"daftar sayuran",
                r"sayuran apa saja"
            ],
            # 12: Kesesuaian hewan hari ini
            "kesesuaian_hewan_hari_ini": [
                r"ingin pelihara (.+) hari ini",
                r"mau ternak (.+) hari ini"
            ],
            # 13: Hewan cocok di provinsi
            "hewan_cocok_provinsi": [
                r"hewan apa saja yang cocok.*di (.+)",
                r"hewan cocok.*provinsi (.+)"
            ],
            # 14: Sayuran cocok di desa
            "sayuran_cocok_desa": [
                r"sayuran apa saja yang cocok.*di (.+)",
                r"sayuran cocok.*desa (.+)"
            ],
            # 15: Kecamatan cocok untuk hewan/sayuran
            "kecamatan_cocok": [
                r"kecamatan.*cocok.*(.+) atau (.+)",
                r"di kecamatan.*cocok.*beternak (.+) atau.*menanam (.+)"
            ],
            # Perbandingan lokasi
            "perbandingan": [
                r"(.+) lebih cocok di (.+) atau (.+)",
                r"(.+) cocok di (.+) atau (.+)"
            ]
        }
    
    def classify_intent(self, user_input):
        """Klasifikasi intent dari input user"""
        input_lower = user_input.lower()
        
        for intent, patterns in self.intent_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, input_lower)
                if match:
                    return {
                        "intent": intent,
                        "groups": match.groups(),
                        "confidence": 0.9
                    }
        
        return {"intent": "unknown", "groups": [], "confidence": 0.0}
    
    def extract_entities(self, user_input, intent_result):
        """Extract entities berdasarkan intent"""
        entities = {
            "lokasi": [],
            "hewan": [],
            "sayuran": [],
            "parameter": {}
        }
        
        groups = intent_result["groups"]
        intent = intent_result["intent"]
        
        if intent == "perbandingan" and len(groups) >= 3:
            entities["hewan"].append(groups[0])
            entities["lokasi"].extend([groups[1], groups[2]])
        
        elif intent == "kesesuaian_hewan_hari_ini" and len(groups) >= 1:
            entities["hewan"].append(groups[0])
            # Extract lokasi dari konteks atau default
        
        elif intent in ["cuaca_lokasi", "suhu_tertinggi", "suhu_terendah"] and len(groups) >= 1:
            entities["lokasi"].append(groups[0])
        
        return entities
    
    def fuzzy_match_lokasi(self, lokasi_raw):
        """Fuzzy matching untuk lokasi"""
        lokasi_normalized = self.data_engine.normalize_text(lokasi_raw)
        
        # Exact match di desa
        if lokasi_normalized in self.data_engine.data_storage["lokasi"]["by_desa"]:
            return {
                "type": "desa",
                "name": lokasi_normalized,
                "data": self.data_engine.data_storage["lokasi"]["by_desa"][lokasi_normalized],
                "confidence": 1.0
            }
        
        # Alias match
        if lokasi_normalized in self.data_engine.data_storage["lokasi"]["by_alias"]:
            return {
                "type": "alias",
                "name": lokasi_normalized,
                "data": self.data_engine.data_storage["lokasi"]["by_alias"][lokasi_normalized],
                "confidence": 0.95
            }
        
        # Check kecamatan
        if lokasi_normalized in self.data_engine.data_storage["lokasi"]["by_kecamatan"]:
            return {
                "type": "kecamatan",
                "name": lokasi_normalized,
                "data": self.data_engine.data_storage["lokasi"]["by_kecamatan"][lokasi_normalized],
                "confidence": 0.9
            }
        
        # Check kotkab
        if lokasi_normalized in self.data_engine.data_storage["lokasi"]["by_kotkab"]:
            return {
                "type": "kotkab",
                "name": lokasi_normalized,
                "data": self.data_engine.data_storage["lokasi"]["by_kotkab"][lokasi_normalized],
                "confidence": 0.85
            }
        
        # Check provinsi
        if lokasi_normalized in self.data_engine.data_storage["lokasi"]["by_provinsi"]:
            return {
                "type": "provinsi",
                "name": lokasi_normalized,
                "data": self.data_engine.data_storage["lokasi"]["by_provinsi"][lokasi_normalized],
                "confidence": 0.8
            }
        
        # Fuzzy string matching
        best_match = None
        best_score = 0.0
        threshold = 0.7
        
        for name in self.data_engine.data_storage["lokasi"]["all_names"]:
            similarity = SequenceMatcher(None, lokasi_normalized, name).ratio()
            if similarity > threshold and similarity > best_score:
                best_score = similarity
                best_match = name
        
        if best_match:
            # Cari data yang sesuai
            if best_match in self.data_engine.data_storage["lokasi"]["by_desa"]:
                return {
                    "type": "desa",
                    "name": best_match,
                    "data": self.data_engine.data_storage["lokasi"]["by_desa"][best_match],
                    "confidence": best_score
                }
        
        return None
    
    def fuzzy_match_hewan(self, hewan_raw):
        """Fuzzy matching untuk hewan"""
        hewan_normalized = self.data_engine.normalize_text(hewan_raw)
        
        # Exact match
        if hewan_normalized in self.data_engine.data_storage["hewan"]["by_nama"]:
            return {
                "name": hewan_normalized,
                "data": self.data_engine.data_storage["hewan"]["by_nama"][hewan_normalized],
                "confidence": 1.0
            }
        
        # Fuzzy match
        best_match = None
        best_score = 0.0
        threshold = 0.7
        
        for name in self.data_engine.data_storage["hewan"]["all_names"]:
            similarity = SequenceMatcher(None, hewan_normalized, name).ratio()
            if similarity > threshold and similarity > best_score:
                best_score = similarity
                best_match = name
        
        if best_match:
            return {
                "name": best_match,
                "data": self.data_engine.data_storage["hewan"]["by_nama"][best_match],
                "confidence": best_score
            }
        
        return None