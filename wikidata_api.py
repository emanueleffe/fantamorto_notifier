import requests
import logging
from data_manager import get_id_from_cache, save_id_to_cache
from telegram_notification import send_telegram_notification

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def find_wikidata_id(DATABASE_FILE, person_name):

    cached_id = get_id_from_cache(DATABASE_FILE, person_name)
    if cached_id:
        return cached_id

    url = 'https://www.wikidata.org/w/api.php'
    params = {
        'action': 'wbsearchentities',
        'format': 'json',
        'language': 'it',
        'search': person_name,
        'type': 'item'
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        for result in data.get('search', []):
            if 'description' in result and 'essere umano' in result['description'].lower():
                q_id = result['id']
                save_id_to_cache(DATABASE_FILE, person_name, q_id)
                return q_id

        if data.get('search'):
            q_id = data['search'][0]['id']
            save_id_to_cache(DATABASE_FILE, person_name, q_id)
            return q_id
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while searching data for '{person_name}': {e}")
        send_telegram_notification(f"Error while searching data for '{person_name}': {e}")
        return -1 
    
    logging.warning(f"Nessun ID Wikidata trovato per '{person_name}'")
    return None

def get_person_data(q_ids):
    results = {}
    if not q_ids:
        return {}
    
    q_ids_chunks = [q_ids[i:i + 50] for i in range(0, len(q_ids), 50)]
    
    for chunk in q_ids_chunks:
        filter_values = ' '.join([f'wd:{q_id}' for q_id in chunk])
        
        query = f"""
        SELECT ?person ?personLabel ?birthDate ?deathDate WHERE {{
          VALUES ?person {{ {filter_values} }}
          OPTIONAL {{ ?person wdt:P569 ?birthDate. }}
          OPTIONAL {{ ?person wdt:P570 ?deathDate. }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "it,en". }}
        }}
        """
        
        url = 'https://query.wikidata.org/sparql'
        
        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            
            for item in data['results']['bindings']:
                person_uri = item['person']['value']
                q_id = person_uri.split('/')[-1]
                person_label = item.get('personLabel', {}).get('value', 'Sconosciuto')
                
                birth_date = item.get('birthDate', {}).get('value')
                death_date = item.get('deathDate', {}).get('value')
                
                death_info = death_date.split('T')[0] if death_date else None
                
                results[q_id] = {
                    'nome': person_label,
                    'data_di_nascita': birth_date.split('T')[0] if birth_date else None,
                    'data_di_morte': death_info,
                    'wikidata_url': person_uri
                }
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Error while contacting wikidata: {e}")
            continue 
            
    return results