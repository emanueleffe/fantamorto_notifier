import sqlite3
import os
import logging
import csv
from telegram_notification import send_telegram_notification, send_specific_telegram_notification
from email_notification import send_email_notification
from datetime import datetime


def calculate_age(birth_date_str, death_date_str):
    if not birth_date_str or not death_date_str:
        return None
    try:
        birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d')
        death_date = datetime.strptime(death_date_str, '%Y-%m-%d')
        age = death_date.year - birth_date.year
        if (death_date.month, death_date.day) < (birth_date.month, birth_date.day):
            age -= 1
        return age
    except ValueError as e:
        logging.error(f"Error while calculating age (dates: {birth_date_str}, {death_date_str}): {e}")
        return None


def execute_sql_file(db_path, sql_file_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        cur.executescript(sql_script)
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error while accessing sql file: {sql_file_path}: {e}")
    finally:
        if conn:
            conn.close()


def create_database_and_tables(DATABASE_FILE):
    try:
        execute_sql_file(DATABASE_FILE, 'db/schema.sql')
        logging.info("db ok.")
    except sqlite3.Error as e:
        logging.error(f"Error while creating db file: {e}")


def create_database_and_tables(DATABASE_FILE):
    try:
        execute_sql_file(DATABASE_FILE, 'db/schema.sql')
        logging.info("db ok.")
    except sqlite3.Error as e:
        logging.error(f"Error while creating db file: {e}")

def get_id_from_cache(DATABASE_FILE, person_name):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT id_wikidata FROM id_cache WHERE nome_originale = ?", (person_name,))
        result = c.fetchone()
        return result[0] if result else None
    except sqlite3.Error as e:
        logging.error(f"Error while reading id from cache table: {e}")
        return None
    finally:
        if conn:
            conn.close()


def save_id_to_cache(DATABASE_FILE, person_name, wikidata_id):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO id_cache (nome_originale, id_wikidata)
            VALUES (?, ?)
        ''', (person_name, wikidata_id))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error while writing id to cache table: {e}")
    finally:
        if conn:
            conn.close()


def get_team_data_from_files(folder):
    """
    Format: "Nome squadra - Nome proprietario - test@email.com - 12345678.csv"
    Returns: dict {"Nome Squadra": {"owner": "...", "people": {...}, "email": "...", "chat_id": "..."}}
    """
    all_people_names = set()
    team_associations = {}

    if not os.path.exists(folder):
        logging.warning(f"Error while reading teams folder: '{folder}' Does not exist. No files to read.")
        return all_people_names, team_associations

    for filename in os.listdir(folder):
        if not filename.endswith('.csv'):
            continue
        
        team_csv_path = os.path.join(folder, filename)
        base_name = os.path.splitext(filename)[0]
        
        parts = [p.strip() for p in base_name.split(' - ')]
        
        if len(parts) < 1:
            continue

        team_name = parts[0]
        owner_name = parts[1] if len(parts) > 1 else "N/A"
        email_notifica = None
        chat_id_notifica = None
        
        if len(parts) > 2:
            for part in parts[2:]:
                if '@' in part:
                    email_notifica = part
                elif part.isdigit():
                    chat_id_notifica = part
        
        team_people = set()
        try:
            with open(team_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip():
                        person_name = row[0].strip()
                        team_people.add(person_name)
                        all_people_names.add(person_name)
                        
            team_associations[team_name] = {
                "owner": owner_name,
                "people": team_people,
                "email": email_notifica,
                "chat_id": chat_id_notifica
            }
        except Exception as e:
            logging.error(f"Error while reading file '{filename}': {e}")
            
    return all_people_names, team_associations


def get_already_processed_info(DATABASE_FILE):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        processed_names = set()
        living_names = set()
        
        c.execute("SELECT nome_originale, data_di_morte FROM persone")
        rows = c.fetchall()

        for row in rows:
            name = row[0].strip()
            death_date = row[1]
            
            processed_names.add(name)
            
            if death_date is None:
                living_names.add(name)
                
        return processed_names, living_names
    except sqlite3.Error as e:
        logging.error(f"Error while reading database file: {e}")
        return set(), set()
    finally:
        if conn:
            conn.close()


def associate_teams(DATABASE_FILE, team_associations):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()

        file_teams = set(team_associations.keys())
        db_teams_rows = c.execute("SELECT id_squadra, nome_squadra FROM squadre").fetchall()
        db_teams_map = {name: id for id, name in db_teams_rows}
        db_teams = set(db_teams_map.keys())

        teams_to_add = file_teams - db_teams
        teams_to_remove = db_teams - file_teams

        if teams_to_add:
            logging.info(f"Adding {len(teams_to_add)} new teams.")
            insert_data = [
                (name, team_associations[name]["owner"], team_associations[name]["email"], team_associations[name]["chat_id"])
                for name in teams_to_add
            ]
            c.executemany("INSERT INTO squadre (nome_squadra, nome_proprietario, email_notifica, tg_chat_id_notifica) VALUES (?, ?, ?, ?)",
                            insert_data)
        
        if teams_to_remove:
            logging.info(f"Removing {len(teams_to_remove)} old teams.")
            c.executemany("DELETE FROM squadre WHERE nome_squadra = ?", [(name,) for name in teams_to_remove])
        
        teams_to_update = file_teams & db_teams
        if teams_to_update:
            logging.info(f"Updating {len(teams_to_update)} existing teams.")
            update_data = [
                (team_associations[name]["owner"], team_associations[name]["email"], team_associations[name]["chat_id"], name)
                for name in teams_to_update
            ]
            c.executemany("UPDATE squadre SET nome_proprietario = ?, email_notifica = ?, tg_chat_id_notifica = ? WHERE nome_squadra = ?",
                            update_data)

        conn.commit()

        team_id_map = {row[1]: row[0] for row in c.execute("SELECT id_squadra, nome_squadra FROM squadre")}
        person_id_map = {row[1]: row[0] for row in c.execute("SELECT id_persona, nome_originale FROM persone")}
        
        desired_association_ids = set()
        for team_name, data in team_associations.items():
            team_id = team_id_map.get(team_name)
            if not team_id:
                logging.warning(f"Team '{team_name}' not found in DB after insertion. Skipping associations.")
                continue
            
            for person_name in data["people"]:
                person_id = person_id_map.get(person_name)
                if person_id: 
                    desired_association_ids.add((team_id, person_id))

        current_association_ids = set(c.execute("SELECT id_squadra, id_persona FROM persone_squadre"))

        links_to_add = desired_association_ids - current_association_ids
        links_to_remove = current_association_ids - desired_association_ids

        if links_to_add:
            c.executemany("INSERT OR IGNORE INTO persone_squadre (id_squadra, id_persona) VALUES (?, ?)", list(links_to_add))
        
        if links_to_remove:
            logging.info(f"Removeing {len(links_to_remove)} obsolete team-person links.")
            c.executemany("DELETE FROM persone_squadre WHERE id_squadra = ? AND id_persona = ?", list(links_to_remove))

        if not links_to_add and not links_to_remove:
            pass 
        
        conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Error while processing db: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def insert_or_update_person(DATABASE_FILE, original_name, data):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute("SELECT id_persona FROM persone WHERE nome_originale = ?", (original_name,))
        existing_row = c.fetchone()
        
        new_data = {
            'nome': data.get('nome', 'Non trovato'),
            'data_di_nascita': data.get('data_di_nascita'),
            'data_di_morte': data.get('data_di_morte'),
            'wikidata_url': data.get('wikidata_url', 'Non trovato'),
            'id_wikidata': data.get('id_wikidata', 'Non trovato')
        }

        if not existing_row:
            c.execute('''
                INSERT OR IGNORE INTO persone (nome_originale, nome_wikidata, data_di_nascita, data_di_morte, link_wikidata, id_wikidata)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (original_name, new_data['nome'], new_data['data_di_nascita'], new_data['data_di_morte'], 
                  new_data['wikidata_url'], new_data['id_wikidata']))
        else:
            c.execute('''
                UPDATE persone
                SET nome_wikidata = ?, data_di_nascita = ?, data_di_morte = ?, 
                    link_wikidata = ?, id_wikidata = ?
                WHERE nome_originale = ?
            ''', (new_data['nome'], new_data['data_di_nascita'], new_data['data_di_morte'],
                  new_data['wikidata_url'], new_data['id_wikidata'], 
                  original_name))
        
        conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Error while inserting data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def process_notifications(DATABASE_FILE):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute('''
            SELECT id_persona, nome_originale, nome_wikidata, data_di_nascita, data_di_morte, link_wikidata 
            FROM persone
            WHERE data_di_morte IS NOT NULL AND notifica_inviata = 0
        ''')
        
        people_to_notify = c.fetchall()
        
        if not people_to_notify:
            return # nothing to do

        for (person_id, original_name, wikidata_name, birth_date, death_date, wikidata_url) in people_to_notify:
            
            logging.warning(f"'{original_name}' is dead. Processing notifications.")
            
            age = calculate_age(birth_date, death_date)
            
            c.execute('''
                SELECT T2.nome_squadra FROM persone_squadre AS T1
                JOIN squadre AS T2 ON T1.id_squadra = T2.id_squadra
                WHERE T1.id_persona = ?
            ''', (person_id,))
            teams = c.fetchall()
            teams_list = [row[0] for row in teams]
            teams_text = "\Teams: " + ", ".join(teams_list) if teams_list else ""

            telegram_message = (
                f"** † *{original_name.upper()}* † **\n\n"
                f"Date of birth: {birth_date}\n"
                f"Date of death: {death_date}\n"
                f"Age at death: {age}\n"
                f"Link: {wikidata_url}"
                f"{teams_text}"
            )
            
            email_subject = f"FantaMorto Notification: {original_name} is dead"
            email_body = (
                f"† {original_name} † \n\n"
                f"Date of birth: {birth_date}\n"
                f"Date of death: {death_date}\n"
                f"Age at death: {age}\n"
                f"Link: {wikidata_url}"
                f"{teams_text}"
            )
            
            is_global_notification_sent = send_telegram_notification(telegram_message)
            
            notifica_inviata_flag = 0
            if is_global_notification_sent:
                notifica_inviata_flag = 1
                logging.info(f"Global notification for '{original_name}' sent. Flag set to 1.")
            else:
                logging.error(f"Unable to send global notification for '{original_name}'. Flag not updated. Trying again next time.")
                continue

            if teams_list:
                logging.info(f"Sending specific notifications to {len(teams_list)} team(s)...")
                
                c.execute(f'''
                    SELECT email_notifica, tg_chat_id_notifica, nome_squadra
                    FROM squadre
                    WHERE nome_squadra IN ({",".join(["?"] * len(teams_list))})
                ''', teams_list)
                
                squadre_notifiche = c.fetchall()
                
                for (email, chat_id, nome_squadra) in squadre_notifiche:
                    
                    if email:
                        logging.info(f"Sending Email to '{nome_squadra}' at {email}...")
                        send_email_notification(email, email_subject, email_body)
                    
                    if chat_id:
                        logging.info(f"Sending Telegram to '{nome_squadra}'...")
                        team_message = f"*FANTAMORTO - {nome_squadra}*\n" + telegram_message
                        send_specific_telegram_notification(chat_id, team_message)

            c.execute("UPDATE persone SET notifica_inviata = 1 WHERE id_persona = ?", (person_id,))
            conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Error while processing notifications: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def remove_unassociated_people(DATABASE_FILE, people_in_csv_files_set):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        c.execute("SELECT nome_originale, id_persona, id_wikidata FROM persone")
        all_people_in_db = c.fetchall()

        people_to_remove = []
        for original_name, person_id, wikidata_id in all_people_in_db:
            if original_name not in people_in_csv_files_set:
                people_to_remove.append((person_id, original_name, wikidata_id))

        if not people_to_remove:
            return

        for person_id, original_name, wikidata_id in people_to_remove:            
            if wikidata_id and wikidata_id != 'Non trovato':
                c.execute("DELETE FROM id_cache WHERE id_wikidata = ?", (wikidata_id,))
            c.execute("DELETE FROM id_cache WHERE nome_originale = ?", (original_name,))
            
            c.execute("DELETE FROM persone WHERE id_persona = ?", (person_id,))

        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error while removing people from db: {e}")
    finally:
        if conn:
            conn.close()