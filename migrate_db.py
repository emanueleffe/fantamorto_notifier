import sqlite3
import shutil
import os

DB_FILE = 'fantamorto.db' # Adjust if your actual db name is different, likely 'fantamorto.db' based on config usually
# However, usually config is in conf/general_config.ini. I should read that to be safe, but hardcoding provided I checked or pass it is fine.
# Let's read it from config for safety.

import configparser
config = configparser.ConfigParser()
config.read('conf/general_config.ini')
DATABASE_FILE = config['GENERALI']['DATABASE_FILE']

def migrate():
    if not os.path.exists(DATABASE_FILE):
        print(f"Database file {DATABASE_FILE} not found. Nothing to migrate.")
        return

    print(f"Migrating {DATABASE_FILE}...")
    
    # Backup
    backup_file = DATABASE_FILE + ".bak"
    shutil.copy(DATABASE_FILE, backup_file)
    print(f"Backup created at {backup_file}")

    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()

    try:
        # Disable foreign keys
        c.execute("PRAGMA foreign_keys=OFF")
        
        c.execute("BEGIN TRANSACTION")

        # 1. Rename old table
        print("Renaming old table...")
        c.execute("ALTER TABLE persone RENAME TO persone_old")

        # 2. Create new table
        print("Creating new table...")
        # Copied from schema.sql but without UNIQUE on id_wikidata
        c.execute("""
            CREATE TABLE IF NOT EXISTS persone (
                id_persona INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                nome_originale TEXT NOT NULL UNIQUE,
                nome_wikidata TEXT,
                data_di_nascita TEXT,
                data_di_morte TEXT,
                link_wikidata TEXT,
                id_wikidata TEXT
            );
        """)

        # 3. Copy data
        print("Copying data...")
        c.execute("""
            INSERT INTO persone (id_persona, nome_originale, nome_wikidata, data_di_nascita, data_di_morte, link_wikidata, id_wikidata)
            SELECT id_persona, nome_originale, nome_wikidata, data_di_nascita, data_di_morte, link_wikidata, id_wikidata
            FROM persone_old
        """)

        # 4. Drop old table
        print("Dropping old table...")
        c.execute("DROP TABLE persone_old")

        c.execute("COMMIT")
        print("Migration successful.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        c.execute("ROLLBACK")
        print("Rolled back changes. Restoring from backup...")
        conn.close()
        shutil.copy(backup_file, DATABASE_FILE)
        print("Restored.")
        return

    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
