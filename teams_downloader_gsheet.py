import os
import re
import pandas as pd

def teams_downloader():
    # --- CONFIGURAZIONE ---
    SHEET_ID = "1_gWArYXL4lSUdIYF2QxXnv59-S39JArhDjh5HvVaMc8"
    GID = "0"
    GOOGLE_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    NOTIFICHE_FILE = "notifiche.csv"
    CORREZIONI_FILE = "correzioni.csv"  # Nuovo file config
    OUTPUT_DIR = "teams"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    def sanitize_filename(name):
        """Rimuove caratteri illegali per il filesystem."""
        return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()

    def clean_key(val):
        """Normalizzazione stringhe: trim + lowercase per join robusto."""
        return str(val).strip().lower() if pd.notna(val) else ""

    # --- 1. CARICAMENTO DATI ---
    
    # A. Caricamento Correzioni (Nuovo blocco)
    corrections_map = {}
    try:
        df_corr = pd.read_csv(CORREZIONI_FILE)
        # Mappa: key normalizzata -> Valore corretto originale
        corrections_map = {
            clean_key(row['Nome scaricato']): str(row['Nome corretto']).strip()
            for _, row in df_corr.iterrows()
            if pd.notna(row['Nome scaricato']) and pd.notna(row['Nome corretto'])
        }
    except FileNotFoundError:
        print(f"Info: '{CORREZIONI_FILE}' non trovato. Nessuna correzione nomi verrà applicata.")
    except Exception as e:
        print(f"Errore caricamento correzioni: {e}")

    # B. Caricamento Notifiche
    try:
        df_notifiche = pd.read_csv(NOTIFICHE_FILE)
        df_notifiche['join_persona'] = df_notifiche['Persona'].apply(clean_key)
        df_notifiche['join_squadra'] = df_notifiche['squadra'].apply(clean_key)
    except FileNotFoundError:
        print(f"File '{NOTIFICHE_FILE}' non trovato. I file verranno generati senza dati di contatto.")
        df_notifiche = pd.DataFrame(columns=['join_persona', 'join_squadra'])

    # C. Caricamento Sheet Teams
    try:
        df_sheet = pd.read_csv(GOOGLE_CSV_URL, header=None)

        header_rows = df_sheet[df_sheet.apply(lambda row: row.astype(str).str.contains('Giocatore', case=False, na=False).any(), axis=1)]
        if header_rows.empty:
            raise ValueError("Header 'Giocatore' non trovato.")
        header_row_idx = header_rows.index[0]

        count_files = 0

        # --- 2. ELABORAZIONE ---
        for col_idx in range(df_sheet.shape[1]):
            cell_value = df_sheet.iloc[header_row_idx, col_idx]

            if str(cell_value).strip() == "Giocatore":
                # A. Metadati dal Sheet
                metadata = df_sheet.iloc[:header_row_idx, col_idx].dropna()

                if len(metadata) >= 1:
                    raw_team = metadata.iloc[0]
                    raw_person = metadata.iloc[1] if len(metadata) > 1 else "Unknown"
                else:
                    continue

                # B. Join Rigorosa (Strict Match)
                email_suffix = ""
                telegram_suffix = ""

                if not df_notifiche.empty:
                    current_team_key = clean_key(raw_team)
                    current_person_key = clean_key(raw_person)

                    match = df_notifiche[
                        (df_notifiche['join_persona'] == current_person_key) &
                        (df_notifiche['join_squadra'] == current_team_key)
                    ]

                    if not match.empty:
                        row = match.iloc[0]
                        if pd.notna(row['email']) and str(row['email']).strip():
                            email_suffix = str(row['email']).strip()
                        if pd.notna(row['telegram_chat_id']) and str(row['telegram_chat_id']).strip():
                            try:
                                telegram_suffix = str(int(float(row['telegram_chat_id'])))
                            except ValueError:
                                telegram_suffix = str(row['telegram_chat_id']).strip()

                # C. Costruzione Filename
                parts = [raw_team, raw_person]
                if email_suffix: parts.append(email_suffix)
                if telegram_suffix: parts.append(telegram_suffix)

                full_name = " - ".join(parts)
                filename = f"{sanitize_filename(full_name)}.csv"
                file_path = os.path.join(OUTPUT_DIR, filename)

                # D. Estrazione e Pulizia Giocatori
                players_series = df_sheet.iloc[header_row_idx + 1:, col_idx].dropna()
                valid_players = [str(p).strip() for p in players_series if str(p).strip() and not str(p).isdigit()]

                if valid_players:
                    # E. Applicazione Correzioni (Nuovo step)
                    # Se la chiave normalizzata è nel dizionario, sostituisci, altrimenti mantieni originale
                    final_players = [
                        corrections_map.get(clean_key(p), p) 
                        for p in valid_players
                    ]

                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(final_players))
                    count_files += 1

    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    teams_downloader()
