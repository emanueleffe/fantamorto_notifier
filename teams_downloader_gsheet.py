import os
import re
import csv
import requests
import io
import logging

def teams_downloader(sheet_id: str, output_dir: str = "teams"):
    # --- CONFIGURAZIONE ---
    SHEET_ID = sheet_id
    GID = "0"
    GOOGLE_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    NOTIFICHE_FILE = "notifiche.csv"
    CORREZIONI_FILE = "correzioni.csv"
    OUTPUT_DIR = output_dir

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    def sanitize_filename(name):
        return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()

    def clean_key(val):
        return str(val).strip().lower() if val else ""
    
    # caricamento correzioni
    corrections_map = {}
    try:
        if os.path.exists(CORREZIONI_FILE):
            with open(CORREZIONI_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('Nome scaricato') and row.get('Nome corretto'):
                        corrections_map[clean_key(row['Nome scaricato'])] = row['Nome corretto'].strip()
        else:
            logging.info(f"Info: '{CORREZIONI_FILE}' not found. No name corrections will be applied.")
    except Exception as e:
        logging.error(f"Error loading corrections: {e}")

    # caricamento notifiche
    notifiche_data = []
    try:
        if os.path.exists(NOTIFICHE_FILE):
            with open(NOTIFICHE_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    notifiche_data.append({
                        'join_persona': clean_key(row.get('Persona')),
                        'join_squadra': clean_key(row.get('squadra')),
                        'email': row.get('email'),
                        'telegram_chat_id': row.get('telegram_chat_id')
                    })
        else:
            logging.warning(f"File '{NOTIFICHE_FILE}' not found. Files will be generated without contact data.")
    except Exception as e:
        logging.error(f"Error loading notifications: {e}")

    # caricamento sheet teams
    try:
        response = requests.get(GOOGLE_CSV_URL)
        response.raise_for_status()
        
        # parse csv from string
        f = io.StringIO(response.content.decode('utf-8'))
        reader = csv.reader(f)
        rows = list(reader)
        
        if not rows:
            raise ValueError("sheet is empty")

        # find header row with "Giocatore"
        header_row_idx = -1
        for i, row in enumerate(rows):
            if any("Giocatore" == str(cell).strip() for cell in row):
                header_row_idx = i
                break
        
        if header_row_idx == -1:
            raise ValueError("Header 'Giocatore' not found.")
        
        count_files = 0
        
        num_cols = len(rows[header_row_idx])
        
        for col_idx in range(num_cols):
            # check bounds for current row
            if col_idx >= len(rows[header_row_idx]):
                continue
                
            cell_value = rows[header_row_idx][col_idx]

            if str(cell_value).strip() == "Giocatore":
                # metadata are in rows before header_row_idx
                metadata = []
                for r in range(header_row_idx):
                    if col_idx < len(rows[r]) and rows[r][col_idx]:
                        metadata.append(rows[r][col_idx])
                
                if len(metadata) >= 1:
                    raw_team = metadata[0]
                    raw_person = metadata[1] if len(metadata) > 1 else "Unknown"
                else:
                    continue

                email_suffix = ""
                telegram_suffix = ""

                if notifiche_data:
                    current_team_key = clean_key(raw_team)
                    current_person_key = clean_key(raw_person)

                    for item in notifiche_data:
                        if item['join_persona'] == current_person_key and item['join_squadra'] == current_team_key:
                            if item['email'] and item['email'].strip():
                                email_suffix = item['email'].strip()
                            if item['telegram_chat_id'] and item['telegram_chat_id'].strip():
                                telegram_suffix = item['telegram_chat_id'].strip()
                            break

                # costruzione filename
                parts = [raw_team, raw_person]
                if email_suffix: parts.append(email_suffix)
                if telegram_suffix: parts.append(telegram_suffix)

                full_name = " - ".join(parts)
                filename = f"{sanitize_filename(full_name)}.csv"
                file_path = os.path.join(OUTPUT_DIR, filename)

                # estrazione e pulizia giocatori
                valid_players = []
                for r in range(header_row_idx + 1, len(rows)):
                    if col_idx < len(rows[r]):
                        val = rows[r][col_idx]
                        if val and str(val).strip() and not str(val).isdigit():
                            valid_players.append(str(val).strip())

                if valid_players:
                    # applicazione correzioni
                    final_players = [
                        corrections_map.get(clean_key(p), p) 
                        for p in valid_players
                    ]

                    with open(file_path, 'w', encoding='utf-8') as f_out:
                        f_out.write('\n'.join(final_players))
                    count_files += 1

    except Exception as e:
        logging.error(f"Errore: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    teams_downloader()
