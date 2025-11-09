import logging
import sys
import os
import configparser
import concurrent.futures

from data_manager import (
    create_database_and_tables,
    get_already_processed_info,
    insert_or_update_person,
    get_team_data_from_files,
    associate_teams,
    remove_unassociated_people,
    queue_new_death_notifications,
    send_queued_notifications
)
from wikidata_api import find_wikidata_id, get_person_data
from telegram_notification import send_telegram_notification

config = configparser.ConfigParser()
config.read('conf/general_config.ini')

DATABASE_FILE = config['GENERALI']['DATABASE_FILE']
LOG_FILE = config['GENERALI']['LOG_FILE']
TEAMS_FOLDER = config['GENERALI']['TEAMS_FOLDER']

MAX_WORKERS_WIKIDATA = 5
MAX_WORKERS_NOTIFICATIONS = 10 


def setup_logging():
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def process_name(name):
    try:
        q_id = find_wikidata_id(DATABASE_FILE, name)
        if q_id == -1:
            return (name, -1)
        if q_id:
            return (name, q_id)
        else:
            return (name, None)
    except Exception as e:
        logging.error(f"Error in thread while searching for '{name}': {e}")
        return (name, -1)


def main():
    setup_logging()

    try:
        create_database_and_tables(DATABASE_FILE)
        
        names_from_teams, team_associations = get_team_data_from_files(TEAMS_FOLDER)
        
        if not names_from_teams:
            logging.info("No teams or players found in the specified folder.")
            return

        processed_names, living_names = get_already_processed_info(DATABASE_FILE)
        
        new_names = names_from_teams - processed_names
        names_to_recheck = living_names & names_from_teams
        names_to_process = new_names | names_to_recheck
        
        if not names_to_process:
            pass # nothing to do
        else:
            logging.info(f"{len(names_to_process)} names to process.")
            
            original_names_map = {}
                        
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_WIKIDATA) as executor:
                future_to_name = {executor.submit(process_name, name): name for name in names_to_process}
                
                for future in concurrent.futures.as_completed(future_to_name):
                    name, q_id = future.result()
                    
                    if q_id == -1:
                        msg = f'Critical error during search for {name} (q_id = -1). Stopping.'
                        logging.error(msg)
                        send_telegram_notification(msg)
                        raise Exception(msg)
                    
                    elif q_id:
                        original_names_map[name] = q_id
                    
                    else:
                        data_to_save = {
                            'nome': 'Not found',
                            'data_di_nascita': None,
                            'data_di_morte': None,
                            'wikidata_url': 'Not found',
                            'id_wikidata': 'Not found'
                        }
                        insert_or_update_person(DATABASE_FILE, name, data_to_save)
                        send_telegram_notification(f"Wikidata ID not found for: {name}")
                        
            q_ids_to_query = list(set(original_names_map.values()))
            
            all_updated_data = get_person_data(q_ids_to_query) 

            for name, q_id in original_names_map.items():
                data_to_save = {}
                if q_id and q_id in all_updated_data:
                    data_to_save = all_updated_data[q_id]
                    data_to_save['id_wikidata'] = q_id

                # --- test case ---
                #if name == "Ornella Vanoni":
                #   logging.warning("!!! test case ornella vanoni !!!")
                #   data_to_save['data_di_morte'] = '2025-01-01'
                # ---end test case ---
                
                insert_or_update_person(DATABASE_FILE, name, data_to_save)
        
        
        associate_teams(DATABASE_FILE, team_associations)
        remove_unassociated_people(DATABASE_FILE, names_from_teams)
        queue_new_death_notifications(DATABASE_FILE)
        send_queued_notifications(DATABASE_FILE, MAX_WORKERS_NOTIFICATIONS)
        
        logging.info(f"End execution.\n\n")
    
    except Exception as e:
        logging.critical(f"Critical error {e}", exc_info=True)
        send_telegram_notification(f"Critical error: {e}")


if __name__ == "__main__":
    main()