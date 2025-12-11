import sqlite3
import os
import logging
import csv
import concurrent.futures
from typing import Optional, Tuple, Set, Dict, List, Any
from datetime import datetime

from telegram_notification import get_global_chat_id, send_specific_telegram_notification
from email_notification import send_email_notification
from database import Database

def calculate_age(birth_date_str: Optional[str], death_date_str: Optional[str]) -> Optional[int]:
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


def execute_sql_file(db: Database, sql_file_path: str) -> None:
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        with db.get_connection() as conn:
            conn.executescript(sql_script)
            
    except Exception as e:
        logging.error(f"Error while accessing sql file: {sql_file_path}: {e}")


def create_database_and_tables(db_path: str) -> None:
    db = Database(db_path)
    try:
        execute_sql_file(db, 'db/schema.sql')
        logging.info("db ok.")
    except Exception as e:
        logging.error(f"Error while creating db file: {e}")


def get_id_from_cache(db_path: str, person_name: str) -> Optional[str]:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
            c.execute("SELECT id_wikidata FROM id_cache WHERE nome_originale = ?", (person_name,))
            result = c.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error while reading id from cache table: {e}")
        return None


def save_id_to_cache(db_path: str, person_name: str, wikidata_id: str) -> None:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
            c.execute('''
                INSERT OR REPLACE INTO id_cache (nome_originale, id_wikidata)
                VALUES (?, ?)
            ''', (person_name, wikidata_id))
    except Exception as e:
        logging.error(f"Error while writing id to cache table: {e}")


def get_team_data_from_files(folder: str) -> Tuple[Set[str], Dict[str, Dict[str, Any]]]:
    """
    Format: "Nome squadra - Nome proprietario - test@email.com - 12345678 - ALL.csv"
    Returns: dict {"Nome Squadra": {"owner": "...", "people": {...}, "email": "...", "chat_id": "...", "notifica_tutti": 0/1}}
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
        notifica_tutti = 0
        
        if len(parts) > 2:
            for part in parts[2:]:
                if '@' in part:
                    email_notifica = part
                elif part.isdigit():
                    chat_id_notifica = part
                elif part.upper() == 'ALL':
                    notifica_tutti = 1
        
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
                "chat_id": chat_id_notifica,
                "notifica_tutti": notifica_tutti
            }
        except Exception as e:
            logging.error(f"Error while reading file '{filename}': {e}")
            
    return all_people_names, team_associations


def get_already_processed_info(db_path: str) -> Tuple[Set[str], Set[str]]:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
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
    except Exception as e:
        logging.error(f"Error while reading database file: {e}")
        return set(), set()


def associate_teams(db_path: str, team_associations: Dict[str, Dict[str, Any]]) -> None:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
            file_teams = set(team_associations.keys())
            db_teams_rows = c.execute("SELECT id_squadra, nome_squadra FROM squadre").fetchall()
            db_teams_map = {name: id for id, name in db_teams_rows}
            db_teams = set(db_teams_map.keys())

            teams_to_add = file_teams - db_teams
            teams_to_remove = db_teams - file_teams

            if teams_to_add:
                logging.info(f"Adding {len(teams_to_add)} new teams.")
                insert_data = [
                    (name, 
                     team_associations[name]["owner"], 
                     team_associations[name]["email"], 
                     team_associations[name]["chat_id"],
                     team_associations[name]["notifica_tutti"])
                    for name in teams_to_add
                ]
                c.executemany("INSERT INTO squadre (nome_squadra, nome_proprietario, email_notifica, tg_chat_id_notifica, notifica_tutti) VALUES (?, ?, ?, ?, ?)",
                                insert_data)
            
            if teams_to_remove:
                c.executemany("DELETE FROM squadre WHERE nome_squadra = ?", [(name,) for name in teams_to_remove])
            
            teams_to_update = file_teams & db_teams
            if teams_to_update:
                update_data = [
                    (team_associations[name]["owner"], 
                     team_associations[name]["email"], 
                     team_associations[name]["chat_id"], 
                     team_associations[name]["notifica_tutti"],
                     name)
                    for name in teams_to_update
                ]
                c.executemany("UPDATE squadre SET nome_proprietario = ?, email_notifica = ?, tg_chat_id_notifica = ?, notifica_tutti = ? WHERE nome_squadra = ?",
                                update_data)

            # Re-fetch map after updates
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
                c.executemany("DELETE FROM persone_squadre WHERE id_squadra = ? AND id_persona = ?", list(links_to_remove))

    except Exception as e:
        logging.error(f"Error while processing db: {e}")


def insert_or_update_person(db_path: str, original_name: str, data: Dict[str, Any]) -> None:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
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

    except Exception as e:
        logging.error(f"Error while inserting data: {e}")


def queue_new_death_notifications(db_path: str) -> None:
    db = Database(db_path)
    GLOBAL_ADMIN_CHAT_ID = get_global_chat_id()
    
    try:
        with db.get_cursor() as c:
            # 1. Global Notifications
            c.execute('''
                SELECT P.id_persona, P.nome_originale, P.data_di_nascita, P.data_di_morte, P.link_wikidata 
                FROM persone P
                LEFT JOIN notifiche_globali NG ON P.id_persona = NG.id_persona
                WHERE P.data_di_morte IS NOT NULL AND (NG.inviata IS NULL OR NG.inviata = 0)
            ''')
            global_to_notify = c.fetchall()
            
            if global_to_notify:
                c.execute("SELECT id_squadra, nome_squadra, email_notifica, tg_chat_id_notifica FROM squadre WHERE notifica_tutti = 1")
                general_subscribers = c.fetchall()
                
                for (person_id, original_name, birth_date, death_date, wikidata_url) in global_to_notify:
                    age = calculate_age(birth_date, death_date)
                    age_text = f"({age} anni)" if age is not None else ""
                    
                    c.execute("SELECT T2.nome_squadra FROM persone_squadre AS T1 JOIN squadre AS T2 ON T1.id_squadra = T2.id_squadra WHERE T1.id_persona = ?", (person_id,))
                    teams_of_dead_person = {row[0] for row in c.fetchall()}
                    teams_str = ', '.join(teams_of_dead_person) if teams_of_dead_person else 'N/A'

                    base_msg = (
                        f"NECROLOGIO FANTAMORTO\n=================================\n\n"
                        f"† *{original_name.upper()}* †\n\n"
                        f"Data di nascita: {birth_date}\n"
                        f"Data di morte: {death_date} {age_text}\n"
                        f"Link Wikidata:\n{wikidata_url}\n"
                        f"---------------------------------\n\n"
                        f"Squadre: {teams_str}"
                    )
                    
                    if GLOBAL_ADMIN_CHAT_ID:
                        admin_msg = "*FANTAMORTO (ADMIN)*\n\n" + base_msg
                        c.execute("INSERT INTO notifiche_coda (tipo, indirizzo, corpo, id_persona) VALUES ('telegram', ?, ?, ?)", (GLOBAL_ADMIN_CHAT_ID, admin_msg, person_id))
                    
                    for (id_squadra, nome_squadra, email, chat_id) in general_subscribers:
                        sub_msg = f"*NOTIFICA GENERALE*\n" + base_msg
                        if email:
                            subject = f"†FantaMorto† Notifica Generale: {original_name}"
                            c.execute("INSERT INTO notifiche_coda (tipo, indirizzo, oggetto, corpo, id_squadra, id_persona) VALUES ('email', ?, ?, ?, ?, ?)", (email, subject, sub_msg, id_squadra, person_id))
                        if chat_id:
                            c.execute("INSERT INTO notifiche_coda (tipo, indirizzo, corpo, id_squadra, id_persona) VALUES ('telegram', ?, ?, ?, ?)", (chat_id, sub_msg, id_squadra, person_id))
                    
                    c.execute("INSERT OR REPLACE INTO notifiche_globali (id_persona, inviata) VALUES (?, 1)", (person_id,))
            
            # 2. Team Specific Notifications
            c.execute('''
                SELECT P.id_persona, P.nome_originale, P.data_di_nascita, P.data_di_morte, P.link_wikidata,
                       S.id_squadra, S.nome_squadra, S.email_notifica, S.tg_chat_id_notifica
                FROM persone P
                JOIN persone_squadre PS ON P.id_persona = PS.id_persona
                JOIN squadre S ON PS.id_squadra = S.id_squadra
                WHERE P.data_di_morte IS NOT NULL AND PS.notifica_inviata = 0
            ''')
            team_notifications = c.fetchall()
            
            for (person_id, original_name, birth_date, death_date, wikidata_url, team_id, team_name, email, chat_id) in team_notifications:
                age = calculate_age(birth_date, death_date)
                age_text = f"({age} anni)" if age is not None else ""
                
                c.execute("SELECT T2.nome_squadra FROM persone_squadre AS T1 JOIN squadre AS T2 ON T1.id_squadra = T2.id_squadra WHERE T1.id_persona = ?", (person_id,))
                all_teams = {row[0] for row in c.fetchall()}
                teams_str = ', '.join(all_teams) if all_teams else 'N/A'

                base_msg = (
                    f"NECROLOGIO FANTAMORTO\n=================================\n\n"
                    f"† *{original_name.upper()}* †\n\n"
                    f"Data di nascita: {birth_date}\n"
                    f"Data di morte: {death_date} {age_text}\n"
                    f"Link Wikidata:\n{wikidata_url}\n"
                    f"---------------------------------\n\n"
                    f"Squadre: {teams_str}"
                )
                
                team_msg = f"*FANTAMORTO*\n\n Squadra: {team_name}\n\n" + base_msg
                email_subject = f"†FantaMorto† Notifica: Decesso - {original_name}"

                if email:
                    c.execute("INSERT INTO notifiche_coda (tipo, indirizzo, oggetto, corpo, id_squadra, id_persona) VALUES ('email', ?, ?, ?, ?, ?)", (email, email_subject, team_msg, team_id, person_id))
                
                if chat_id:
                    c.execute("INSERT INTO notifiche_coda (tipo, indirizzo, corpo, id_squadra, id_persona) VALUES ('telegram', ?, ?, ?, ?)", (chat_id, team_msg, team_id, person_id))
                
                c.execute("UPDATE persone_squadre SET notifica_inviata = 1 WHERE id_squadra = ? AND id_persona = ?", (team_id, person_id))

    except Exception as e:
        logging.error(f"Error while queueing notifications: {e}")


def _process_queue_job(job: Tuple) -> Tuple[int, bool]:
    (id_coda, tipo, indirizzo, oggetto, corpo) = job
    
    success = False
    if tipo == 'email':
        success = send_email_notification(indirizzo, oggetto, corpo)
    elif tipo == 'telegram':
        success = send_specific_telegram_notification(indirizzo, corpo)
        
    return id_coda, success


def send_queued_notifications(db_path: str, MAX_WORKERS: int = 5) -> None:
    db = Database(db_path)
    MAX_RETRIES = 5
    
    try:
        with db.get_cursor() as c:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # job is now (id, tipo, addr, subj, body, id_squadra, id_persona, attempts)
                # fetch all columns, pass first 5 to processor
                c.execute("SELECT id_coda, tipo, indirizzo, oggetto, corpo, id_squadra, id_persona, tentativi FROM notifiche_coda")
                jobs = c.fetchall()

                if not jobs:
                    return

                future_to_job = {executor.submit(_process_queue_job, job[:5]): job for job in jobs}
                
                for future in concurrent.futures.as_completed(future_to_job):
                    original_job = future_to_job[future]
                    job_id, tipo, indirizzo, oggetto, corpo, id_squadra, id_persona, tentativi = original_job
                    
                    _, success = future.result()
                    
                    if success:
                        logging.info(f"Notification {job_id} sent successfully.")
                        jobs_to_delete.append((job_id,))
                        history_entries.append((tipo, indirizzo, oggetto, corpo, 'inviato', id_squadra, id_persona))
                    else:
                        new_attempts = tentativi + 1
                        if new_attempts >= MAX_RETRIES:
                            logging.error(f"Notification {job_id} failed permanently after {new_attempts} attempts.")
                            jobs_to_delete.append((job_id,))
                            history_entries.append((tipo, indirizzo, oggetto, corpo, 'fallito', id_squadra, id_persona))
                        else:
                            logging.warning(f"Notification {job_id} failed. Retry {new_attempts}/{MAX_RETRIES}.")
                            jobs_to_update_retry.append((new_attempts, job_id))

            if jobs_to_delete:
                c.executemany("DELETE FROM notifiche_coda WHERE id_coda = ?", jobs_to_delete)
            
            if jobs_to_update_retry:
                c.executemany("UPDATE notifiche_coda SET tentativi = ? WHERE id_coda = ?", jobs_to_update_retry)
                
            if history_entries:
                c.executemany('''
                    INSERT INTO notifiche_storico (tipo, indirizzo, oggetto, corpo, stato, id_squadra, id_persona)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', history_entries)

    except Exception as e:
        logging.error(f"Errore durante l'invio della coda di notifiche: {e}")


def remove_unassociated_people(db_path: str, people_in_csv_files_set: Set[str]) -> None:
    db = Database(db_path)
    try:
        with db.get_cursor() as c:
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

    except Exception as e:
        logging.error(f"Error while removing people from db: {e}")