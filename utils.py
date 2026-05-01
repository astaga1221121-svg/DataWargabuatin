import os
import json
import re
from typing import List, Dict, Union
import unicodedata
from fuzzywuzzy import fuzz

def normalize_text(text: str) -> str:
    text = unicodedata.normalize('NFKD', text.lower())
    text = re.sub(r'[^a-z0-9\s]', '', text)
    return text.strip()

def load_json_files(folder_path: str) -> List[Dict]:
    data = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as f:
                data.append(json.load(f))
    return data

def fuzzy_match(query: str, choices: List[str], threshold: int = 70) -> List[str]:
    normalized_query = normalize_text(query)
    matches = []
    for choice in choices:
        if fuzz.ratio(normalized_query, normalize_text(choice)) >= threshold:
            matches.append(choice)
    return matches

def find_location(data: List[Dict], search_term: str) -> List[Dict]:
    results = []
    search_term = normalize_text(search_term)
    
    for loc in data:
        match_fields = [
            loc.get('provinsi', ''),
            loc.get('kotkab', ''),
            loc.get('kecamatan', ''),
            loc.get('desa', ''),
            *loc.get('alias', []),
            str(loc.get('lon', '')),
            str(loc.get('lat', '')),
            loc.get('timezone', '')
        ]
        
        if any(search_term in normalize_text(field) for field in match_fields):
            results.append(loc)
    
    return results

def load_json_files(folder_path: str) -> list:
    """Load all JSON files from a directory into a list of dictionaries"""
    data = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data.append(json.load(f))
            except json.JSONDecodeError as e:
                print(f"Error loading {filename}: {e}")
            except Exception as e:
                print(f"Unexpected error loading {filename}: {e}")
    return data


def extract_entities(text: str) -> Dict:
    entities = {
        'lokasi': [],
        'hewan': [],
        'sayuran': [],
        'parameter': [],
        'waktu': []
    }
    
    # Predefined lists
    parameters = ['suhu', 'kelembapan', 'cuaca', 't_max', 't_min', 't_avg', 'hu_avg', 'cuaca_dominan']
    time_terms = ['hari ini', 'sekarang', 'saat ini', 'besok', 'kemarin']
    
    words = normalize_text(text).split()
    
    for word in words:
        if word in parameters:
            entities['parameter'].append(word)
        elif word in time_terms:
            entities['waktu'].append(word)
    
    return entities