CREATE TABLE IF NOT EXISTS persone (
    id_persona INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    nome_originale TEXT NOT NULL UNIQUE,
    nome_wikidata TEXT,
    data_di_nascita TEXT,
    data_di_morte TEXT,
    link_wikidata TEXT,
    id_wikidata TEXT UNIQUE
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
    tg_chat_id_notifica TEXT,
    notifica_tutti INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS persone_squadre (
    id_squadra INTEGER NOT NULL,
    id_persona INTEGER NOT NULL,
    notifica_inviata INTEGER DEFAULT 0,
    PRIMARY KEY (id_squadra, id_persona),
    FOREIGN KEY (id_squadra) REFERENCES squadre(id_squadra) ON DELETE CASCADE,
    FOREIGN KEY (id_persona) REFERENCES persone(id_persona) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS notifiche_globali (
    id_persona INTEGER NOT NULL PRIMARY KEY,
    inviata INTEGER DEFAULT 0,
    FOREIGN KEY (id_persona) REFERENCES persone(id_persona) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS notifiche_coda (
    id_coda INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL, -- 'email' or 'telegram'
    indirizzo TEXT NOT NULL, -- Email or Chat ID
    oggetto TEXT, -- email only
    corpo TEXT NOT NULL,
    stato TEXT DEFAULT 'in_attesa', -- 'in_attesa' o 'fallito'
    tentativi INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS notifiche_storico (
    id_storico INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL,
    indirizzo TEXT NOT NULL,
    oggetto TEXT,
    corpo TEXT NOT NULL,
    data_invio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stato TEXT DEFAULT 'inviato'
);