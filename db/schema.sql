CREATE TABLE IF NOT EXISTS persone (
    id_persona INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    nome_originale TEXT NOT NULL UNIQUE,
    nome_wikidata TEXT,
    data_di_nascita TEXT,
    data_di_morte TEXT,
    link_wikidata TEXT,
    id_wikidata TEXT UNIQUE,
    notifica_inviata INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS id_cache (
    id_cache INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    id_wikidata TEXT UNIQUE,
    nome_originale TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS squadre (
    id_squadra INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    nome_squadra TEXT NOT NULL UNIQUE,
    nome_proprietario TEXT,
    email_notifica TEXT,
    tg_chat_id_notifica TEXT
);

CREATE TABLE IF NOT EXISTS persone_squadre (
    id_squadra INTEGER NOT NULL,
    id_persona INTEGER NOT NULL,
    PRIMARY KEY (id_squadra, id_persona),
    FOREIGN KEY (id_squadra) REFERENCES squadre(id_squadra) ON DELETE CASCADE,
    FOREIGN KEY (id_persona) REFERENCES persone(id_persona) ON DELETE CASCADE
);