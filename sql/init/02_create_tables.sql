-- =================================================================
-- DK-DE-OnPrem-CustomPlatform
-- Script : 02_create_tables.sql
-- Desciption : Création de toutes les tables
--              Bronze / Silver / Gold / Audit
-- =================================================================



-- =================================================================
-- SCHEMA BRONZE : Données brutes ingérées (aucune transformation)
-- =================================================================

-- Table de staging pour les déclarations douanières brutes
CREATE TABLE IF NOT EXISTS bronze.stg_declarations_douane (
    id_raw                  SERIAL PRIMARY KEY,
    source_systeme          VARCHAR(50),
    source_fichier          VARCHAR(255),
    source_url              TEXT,
    date_ingestion          TIMESTAMP DEFAULT NOW(),
    numero_declaration      VARCHAR(50),
    date_declaration        VARCHAR(20),
    code_pays_origine       VARCHAR(10),
    code_pays_destination   VARCHAR(10),    
    code_hs_marchandise     VARCHAR(8),     
    poids_brut              VARCHAR(50),
    valeur_brute            VARCHAR(50),
    statut_brut             VARCHAR(50),  
    raw_json                JSONB
);

-- Table de staging pour les mouvements de navires 
CREATE TABLE IF NOT EXISTS bronze.stg_mouvements_navires (
    id_raw                   SERIAL PRIMARY KEY,
    source_systeme           VARCHAR(50),
    source_fichier           VARCHAR(255),
    source_url               TEXT,
    date_ingestion           TIMESTAMP DEFAULT NOW(),
    numero_mmsi              VARCHAR(9),
    numero_imo               VARCHAR(20),
    nom_navire               VARCHAR(100),
    latitude_brute           VARCHAR(50),
    longitude_brute          VARCHAR(50),
    vitesse_noeud            VARCHAR(50),
    port_destination         VARCHAR(100),
    temps_position           VARCHAR(50),
    raw_json                 JSONB
);

-- Table staging pour les flux commerciaux (UN Comtrade)

CREATE TABLE IF NOT EXISTS bronze.stg_flux_commerciaux (
    id_raw                     SERIAL PRIMARY KEY,
    source_systeme             VARCHAR(50),
    source_fichier             VARCHAR(255),
    source_url                 TEXT,
    date_ingestion             TIMESTAMP DEFAULT NOW(),
    periode                    VARCHAR(6),  -- format "202401"
    code_pays_exportateur      VARCHAR(3),
    code_pays_importateur      VARCHAR(3),
    sens_flux                  VARCHAR(10),  -- "Import" ou "Export"
    code_hs_marchandise        VARCHAR(8),
    valeur_usd_brute           VARCHAR(50),
    poids_kg_brut              VARCHAR(50),
    raw_json                   JSONB    

);

-- Table staging pour les manifestes des conteneurs au format standard

CREATE TABLE IF NOT EXISTS bronze.stg_manifestes_conteneurs (
    id_raw                     SERIAL PRIMARY KEY,
    source_systeme             VARCHAR(50),
    source_fichier             VARCHAR(255),
    source_url                 TEXT,
    date_ingestion             TIMESTAMP DEFAULT NOW(),
    numero_conteneur           CHAR(11),
    type_conteneur             VARCHAR(10),
    statut_brut                VARCHAR(50),
    port_chargement            VARCHAR(100),
    port_dechargement          VARCHAR(100),
    date_arrivee_brute         VARCHAR(50),
    date_depart_brute          VARCHAR(50),
    poids_brut                 VARCHAR(50),
    nom_navire                 VARCHAR(100),
    raw_json                   JSONB

);

-- Table staging pour les liquidations douanières

CREATE TABLE IF NOT EXISTS bronze.stg_taxes_douane (
    id_raw                     SERIAL PRIMARY KEY,
    source_systeme             VARCHAR(50),
    source_fichier             VARCHAR(255),
    source_url                 TEXT,
    date_ingestion             TIMESTAMP DEFAULT NOW(),
    numero_liquidation         VARCHAR(50),
    numero_declaration         VARCHAR(50),
    type_droit_brut            VARCHAR (50),
    taux_brut                  VARCHAR(50),
    montant_brut               VARCHAR(50),
    base_imposition_brute      VARCHAR(50),
    devise                     CHAR(3),
    date_liquidation_brute     VARCHAR(50),
    raw_json                   JSONB

);


-- =================================================================
-- SCHEMA SILVER : Données nettoyées et tranformées
-- =================================================================

-- Table nettoyée pour les déclarations douanières

CREATE TABLE IF NOT EXISTS silver.declarations_douane(
    id_declaration             SERIAL PRIMARY KEY,
    source_systeme             VARCHAR(50),       
    source_fichier             VARCHAR (255),               
    date_ingestion             TIMESTAMP DEFAULT NOW(),
    numero_declaration         VARCHAR(50) NOT NULL,
    date_declaration           DATE NOT NULL,  
    code_pays_origine          CHAR(2) NOT NULL,
    code_pays_destination      CHAR(2) NOT NULL,
    code_hs_marchandise        VARCHAR(8) NOT NULL,
    poids_kg_net               NUMERIC(15,3) CHECK(poids_kg_net >0),
    valeur_euro                NUMERIC(15,2) CHECK(valeur_euro > 0),
    statut                     VARCHAR (20) NOT NULL CHECK (statut IN ('ACCEPTE', 'REFUSE', 'EN_COURS')),
    ligne_valide               BOOLEAN DEFAULT TRUE,
    notes_validation           TEXT
);

-- Table de staging pour les mouvements de navires 
CREATE TABLE IF NOT EXISTS silver.mouvements_navires (
    id_mouvement             SERIAL PRIMARY KEY,
    source_systeme           VARCHAR(50),
    source_fichier           VARCHAR(255),
    date_ingestion           TIMESTAMP DEFAULT NOW(),
    numero_mmsi              VARCHAR(9) NOT NULL,
    numero_imo               VARCHAR(7),
    nom_navire               VARCHAR(100),
    latitude                 NUMERIC(9,6) CHECK (latitude  BETWEEN -90 and 90),
    longitude                NUMERIC(9,6) CHECK (longitude  BETWEEN -180 and 180),
    vitesse_noeud            NUMERIC(5,1) CHECK (vitesse_noeud >=0),
    port_destination         VARCHAR(100),
    temps_position           TIMESTAMP NOT NULL,
    ligne_valide             BOOLEAN DEFAULT TRUE,
    notes_validation         TEXT
);


-- Table silver pour les flux commerciaux

CREATE TABLE IF NOT EXISTS silver.stg_flux_commerciaux (
    id_flux                     SERIAL PRIMARY KEY,
    source_systeme             VARCHAR(50),
    source_fichier             VARCHAR(255),
    date_ingestion             TIMESTAMP DEFAULT NOW(),
    annee                       SMALLINT NOT NULL,
    mois                       SMALLINT CHECK(mois BETWEEN 1 AND 12),
    code_pays_exportateur      CHAR(3) NOT NULL,
    code_pays_importateur      CHAR(3) NOT NULL,
    sens_flux                  VARCHAR(10) CHECK (sens_flux IN ('IMPORT', 'EXPORT')),
    code_hs_marchandise        CHAR(6) NOT NULL,
    valeur_usd                 NUMERIC(18,2) CHECK(valeur_usd >=0),
    poids_kg                   NUMERIC(18,3) CHECK(poids_kg >0),
    ligne_valide             BOOLEAN DEFAULT TRUE,
    notes_validation         TEXT

);


-- Table silver pour les manifestes des conteneurs au format standard
CREATE TABLE IF NOT EXISTS silver.manifestes_conteneurs (
    id_manifeste             SERIAL PRIMARY KEY,
    source_systeme           VARCHAR(50),
    source_fichier           VARCHAR(255),
    date_ingestion           TIMESTAMP DEFAULT NOW(),
    numero_conteneur         CHAR(11) NOT NULL,
    type_conteneur           VARCHAR(10) NOT NULL,
    statut                   VARCHAR (20) NOT NULL CHECK (statut IN ('EN_TRANSIT','STOCKE','LIVRE','BLOQUE')),
    code_port_chargement     VARCHAR(5) NOT NULL,
    code_port_dechargement   VARCHAR(5) NOT NULL,
    date_arrivee             TIMESTAMP,
    date_depart              TIMESTAMP,
    poids_kg                 NUMERIC(10,3) CHECK(poids_kg >0),
    numero_imo               VARCHAR(7),
    ligne_valide             BOOLEAN DEFAULT TRUE,
    notes_validation         TEXT,
    CONSTRAINT chk_dates CHECK (date_depart >= date_arrivee OR date_depart IS NULL)
);

-- Table silver pour les liquidations douanières
CREATE TABLE IF NOT EXISTS silver.taxes_douane (
    id_liquidation           SERIAL PRIMARY KEY,
    source_systeme           VARCHAR(50),
    source_fichier           VARCHAR(255),
    date_ingestion           TIMESTAMP DEFAULT NOW(),
    numero_liquidation       VARCHAR(50) NOT NULL UNIQUE,
    numero_declaration       VARCHAR(50) NOT NULL,
    type_droit               VARCHAR (30) CHECK (type_droit IN ('DROIT_DOUANE', 'TVA', 'ACCISE', 'AUTRE')),
    taux                     NUMERIC(6,3) CHECK(taux BETWEEN 0 AND 100),
    montant                  NUMERIC(15,2) NOT NULL CHECK(montant > 0),
    base_imposition_euro     NUMERIC(15,2) CHECK (base_imposition_euro >=0),
    devise                   CHAR(3) DEFAULT 'EUR' CHECK (devise IN ('EUR', 'USD')),
    date_liquidation         DATE NOT NULL,
    ligne_valide             BOOLEAN DEFAULT TRUE,
    notes_validation         TEXT
);



-- =================================================================
-- SCHEMA GOLD : Données destinées à l'analyse et à la visualisation
-- (agrégations, KPIs, etc.)
-- =================================================================

-- Table date pour les analyses temporelles
CREATE TABLE IF NOT EXISTS gold.dim_date (
    id_date         INTEGER PRIMARY KEY,
    date_complete   DATE NOT NULL UNIQUE,
    annee           SMALLINT NOT NULL,
    mois            SMALLINT NOT NULL,
    semaine         SMALLINT NOT NULL CHECK (semaine BETWEEN 1 AND 53),
    jour            SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL CHECK (trimestre IN (1, 2, 3, 4)),
    jour_semaine    SMALLINT NOT NULL,
    nom_mois        VARCHAR(20) NOT NULL,
    nom_jour        VARCHAR(20) NOT NULL,
    est_weekend     BOOLEAN NOT NULL,
    est_jour_ferie  BOOLEAN DEFAULT FALSE
);

-- Table pays pour les analyses géographiques
CREATE TABLE IF NOT EXISTS gold.dim_pays (
    id_pays         SERIAL PRIMARY KEY,
    code_iso2       CHAR(2) NOT NULL UNIQUE,
    code_iso3       CHAR(3) NOT NULL UNIQUE,
    nom_pays_fr     VARCHAR(100) NOT NULL,
    nom_pays_en     VARCHAR(100),
    region          VARCHAR(50),
    sous_region     VARCHAR(50),
    zone_douaniere  VARCHAR(20) CHECK (zone_douaniere IN ('UE', 'NON_UE', 'OCDE', 'AUTRE')),
    continent       VARCHAR(50),
    est_actif       BOOLEAN DEFAULT TRUE
);

-- Table port pour les analyses portuaires
CREATE TABLE IF NOT EXISTS gold.dim_port (
    id_port         SERIAL PRIMARY KEY,
    code_unlocode   CHAR(5) NOT NULL UNIQUE,
    nom_port        VARCHAR(100) NOT NULL,
    code_pays_iso2  CHAR(2) NOT NULL,
    ville           VARCHAR(100),
    latitude        NUMERIC(9,6) CHECK (latitude  BETWEEN -90 and 90),
    longitude       NUMERIC(9,6) CHECK (longitude  BETWEEN -180 and 180),
    region          VARCHAR(50),
    type_port       VARCHAR(20) CHECK (type_port IN ('MARITIME', 'FLUVIAL', 'MIXTE')),
    capacite_teu    INTEGER CHECK (capacite_teu >0),
    est_actif       BOOLEAN DEFAULT TRUE
);

-- Table navire pour les analyses liées aux navires
CREATE TABLE IF NOT EXISTS gold.dim_navire (
    id_navire             SERIAL PRIMARY KEY,
    numero_mmsi           VARCHAR(9) NOT NULL UNIQUE,
    numero_imo            VARCHAR(7) UNIQUE,
    code_pavillon_iso2    CHAR(2) NOT NULL,
    nom_navire            VARCHAR(100) NOT NULL,
    type_navire           VARCHAR(50) CHECK (type_navire IN ('PORTE_CONTENEURS', 'PETROLIER', 'GAZIER','FERRY', 'VRAQUIER', 'PASSAGERS', 'AUTRE')),
    tonnage               NUMERIC(15,3) CHECK (tonnage >=0),
    capacite_teu          INTEGER CHECK (capacite_teu >0),
    annee_construction    SMALLINT CHECK (annee_construction > 1800 AND annee_construction <= EXTRACT(YEAR FROM CURRENT_DATE)),
    est_actif             BOOLEAN DEFAULT TRUE
);

-- Table des marchadises pour les analyses liées aux produits
CREATE TABLE IF NOT EXISTS gold.dim_marchandise (
    id_marchandise        SERIAL PRIMARY KEY,
    code_hs6              CHAR(6) NOT NULL UNIQUE,
    code_hs4              CHAR(4) NOT NULL,
    code_hs2              CHAR(2) NOT NULL,
    description_fr        TEXT NOT NULL,
    description_en        TEXT,
    categorie             VARCHAR(50),
    famille               VARCHAR(50),
    poids_unitaire_kg     NUMERIC(10,3) CHECK (poids_unitaire_kg >0),
    valeur_unitaire_euro  NUMERIC(10,2) CHECK (valeur_unitaire_euro >0),
    est_dangereux        BOOLEAN DEFAULT FALSE,
    est_actif             BOOLEAN DEFAULT TRUE
);

-- Table des clients pour les analyses liées aux acteurs économiques
CREATE TABLE IF NOT EXISTS gold.dim_client (
    id_client                       SERIAL PRIMARY KEY,
    code_client                     VARCHAR(20) NOT NULL UNIQUE,
    raison_sociale                  VARCHAR(250) NOT NULL,
    type_client                     VARCHAR(20) CHECK (type_client IN ('EXPORTATEUR', 'IMPORTATEUR', 'TRANSITAIRE', 'COMMISSIONNAIRE','AUTRE')),
    code_pays_iso2                  CHAR(2),
    ville                           VARCHAR(100),
    secteur_activite                VARCHAR(50),
    numero_agrement_douane          VARCHAR(50) UNIQUE,
    chiffre_affaires_million_euro   NUMERIC(15,2) CHECK (chiffre_affaires_million_euro >=0),
    est_actif                       BOOLEAN DEFAULT TRUE,
    date_creation                   DATE DEFAULT CURRENT_DATE
);

-- Table des faits importations pour les analyses importations
CREATE TABLE IF NOT EXISTS gold.fact_importations (
    id_importation         BIGSERIAL PRIMARY KEY,
    id_date                INTEGER NOT NULL,
    id_pays_origine        INTEGER NOT NULL,
    id_pays_destination    INTEGER NOT NULL,
    id_port_entree         INTEGER,
    id_marchandise         INTEGER NOT NULL,
    id_client              INTEGER,
    id_navire              INTEGER,
    numero_declaration     VARCHAR(50) NOT NULL,
    poids_kg               NUMERIC(15,3) NOT NULL CHECK(poids_kg >0),
    valeur_euro            NUMERIC(15,2) NOT NULL CHECK(valeur_euro > 0),
    montant_taxes_euro     NUMERIC(15,2) DEFAULT 0 CHECK(montant_taxes_euro >=0),
    nombre_conteneurs      SMALLINT DEFAULT 0 CHECK (nombre_conteneurs >=0),
    delai_dedouanement_jours  SMALLINT DEFAULT 0 CHECK (delai_dedouanement_jours >=0),
    date_chargement           TIMESTAMP DEFAULT NOW(),
    ligne_valide           BOOLEAN DEFAULT TRUE,
    notes_validation       TEXT,
    FOREIGN KEY (id_date) REFERENCES gold.dim_date(id_date),
    FOREIGN KEY (id_pays_origine) REFERENCES gold.dim_pays(id_pays),
    FOREIGN KEY (id_pays_destination) REFERENCES gold.dim_pays(id_pays),
    FOREIGN KEY (id_marchandise) REFERENCES gold.dim_marchandise(id_marchandise),
    FOREIGN KEY (id_client) REFERENCES gold.dim_client(id_client),
    FOREIGN KEY (id_port_entree) REFERENCES gold.dim_port(id_port),
    FOREIGN KEY (id_navire) REFERENCES gold.dim_navire(id_navire)
);

-- Table des faits exportations pour les analyses exportations
CREATE TABLE IF NOT EXISTS gold.fact_exportations (
    id_exportation         BIGSERIAL PRIMARY KEY,
    id_date                INTEGER NOT NULL,
    id_pays_origine        INTEGER NOT NULL,
    id_pays_destination    INTEGER NOT NULL,
    id_port_sortie         INTEGER,
    id_marchandise         INTEGER NOT NULL,
    id_client              INTEGER,
    id_navire              INTEGER,
    pays_transit           VARCHAR(100),
    numero_declaration     VARCHAR(50) NOT NULL,
    poids_kg               NUMERIC(15,3) NOT NULL CHECK(poids_kg >0),
    valeur_euro            NUMERIC(15,2) NOT NULL CHECK(valeur_euro > 0),
    montant_taxes_euro     NUMERIC(15,2) DEFAULT 0 CHECK(montant_taxes_euro >=0),
    nombre_conteneurs      SMALLINT DEFAULT 0 CHECK (nombre_conteneurs >=0),
    date_chargement        TIMESTAMP DEFAULT NOW(),
    ligne_valide           BOOLEAN DEFAULT TRUE,
    notes_validation       TEXT,
    FOREIGN KEY (id_date) REFERENCES gold.dim_date(id_date),
    FOREIGN KEY (id_pays_origine) REFERENCES gold.dim_pays(id_pays),
    FOREIGN KEY (id_pays_destination) REFERENCES gold.dim_pays(id_pays),
    FOREIGN KEY (id_marchandise) REFERENCES gold.dim_marchandise(id_marchandise),
    FOREIGN KEY (id_client) REFERENCES gold.dim_client(id_client),
    FOREIGN KEY (id_port_sortie) REFERENCES gold.dim_port(id_port),
    FOREIGN KEY (id_navire) REFERENCES gold.dim_navire(id_navire)
);

-- Table des faits mouvements de conteneurs pour les analyses logistiques
CREATE TABLE IF NOT EXISTS gold.fact_mouvements_conteneurs (
    id_mouvement_conteneur   BIGSERIAL PRIMARY KEY,
    id_date_arrivee        INTEGER NOT NULL,
    id_date_depart         INTEGER,
    id_port                INTEGER NOT NULL,
    id_navire              INTEGER,
    id_marchandise         INTEGER,
    numero_conteneur       CHAR(11) NOT NULL,
    type_conteneur         VARCHAR(10) CHECK (type_conteneur IN ('20Pieds', '40Pieds', '40Pieds_HC', '45Pieds','REFRIGERE','AUTRE')),
    statut                 VARCHAR (20) NOT NULL CHECK (statut IN ('EN_TRANSIT','STOCKE','LIVRE','BLOQUE')),
    date_chargement        TIMESTAMP DEFAULT NOW(), 
    duree_sejour_jours     NUMERIC(8,2) DEFAULT 0 CHECK (duree_sejour_jours >=0),
    poids_kg               NUMERIC(10,3) CHECK(poids_kg >0),
    terminal               VARCHAR(50),
    FOREIGN KEY (id_date_arrivee) REFERENCES gold.dim_date(id_date),
    FOREIGN KEY (id_date_depart) REFERENCES gold.dim_date(id_date),
    FOREIGN KEY (id_port) REFERENCES gold.dim_port(id_port),
    FOREIGN KEY (id_navire) REFERENCES gold.dim_navire(id_navire),
    FOREIGN KEY (id_marchandise) REFERENCES gold.dim_marchandise(id_marchandise)
);

-- Table des faits des recettes douanières pour les analyses fiscales
CREATE TABLE IF NOT EXISTS gold.fact_recettes_douane (
    id_recette_douane       BIGSERIAL PRIMARY KEY,
    id_date_liquidation    INTEGER NOT NULL,
    id_pays_origine        INTEGER,
    id_client              INTEGER,
    id_marchandise         INTEGER NOT NULL,
    numero_liquidation    VARCHAR(50) NOT NULL UNIQUE,
    numero_declaration    VARCHAR(50) NOT NULL,
    type_droit           VARCHAR (30) CHECK (type_droit IN ('DROIT_DOUANE', 'TVA', 'ACCISE', 'AUTRE')),
    taux_pct              NUMERIC(6,3) CHECK(taux_pct BETWEEN 0 AND 100),
    montant_euro          NUMERIC(15,2) NOT NULL CHECK(montant_euro >= 0),
    base_imposition_euro NUMERIC(15,2) CHECK (base_imposition_euro >=0),
    devise               CHAR(3) DEFAULT 'EUR' CHECK (devise IN ('EUR', 'USD')),
    date_chargement      TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (id_date_liquidation) REFERENCES gold.dim_date(id_date),
    FOREIGN KEY (id_pays_origine) REFERENCES gold.dim_pays(id_pays),
    FOREIGN KEY (id_client) REFERENCES gold.dim_client(id_client),
    FOREIGN KEY (id_marchandise) REFERENCES gold.dim_marchandise(id_marchandise)
);

-- =================================================================
-- SCHEMA AUDIT : Données d'audit pour le suivi des processus ETL, 
-- des accès, etc.
-- =================================================================

-- Table d'audit pour les pipelines ETL
CREATE TABLE IF NOT EXISTS audit.audit_pipelines (
    id_execution          BIGSERIAL PRIMARY KEY,
    nom_dag               VARCHAR(100) NOT NULL,
    nom_tache             VARCHAR(100) NOT NULL,
    statut_execution      VARCHAR(20) NOT NULL CHECK (statut_execution IN ('SUCCES', 'ECHEC', 'EN_COURS', 'ANNULE')),
    date_debut            TIMESTAMP NOT NULL,
    date_fin              TIMESTAMP,
    duree_execution_sec   NUMERIC(10,2) CHECK (duree_execution_sec >=0),
    nb_lignes_lues        INTEGER DEFAULT 0 CHECK (nb_lignes_lues >=0),
    nb_lignes_inserees    INTEGER DEFAULT 0 CHECK (nb_lignes_inserees  >=0),
    nb_lignes_rejetees    INTEGER DEFAULT 0 CHECK (nb_lignes_rejetees  >=0),
    source_fichier        VARCHAR(255),
    utilisateur_systeme   VARCHAR(50),
    message_execution     TEXT,
    date_creation         TIMESTAMP DEFAULT NOW()
);

-- Table d'audit logs erreurs
CREATE TABLE IF NOT EXISTS audit.logs_erreurs (
    id_erreur            BIGSERIAL PRIMARY KEY,
    id_execution          BIGINT,
    nom_dag               VARCHAR(100) NOT NULL,
    nom_tache             VARCHAR(100) NOT NULL,
    niveau_erreur         VARCHAR(20) NOT NULL CHECK (niveau_erreur IN ('ERROR', 'WARNING', 'INFO')),
    message_erreur        TEXT NOT NULL,
    ligne_source          TEXT,
    date_erreur           TIMESTAMP DEFAULT NOW()
);  

-- Table d'audit des accès
CREATE TABLE IF NOT EXISTS audit.audit_acces (
    id_acces             BIGSERIAL PRIMARY KEY,
    nom_utilisateur       VARCHAR(50) NOT NULL,
    role_utilisateur      VARCHAR(50) NOT NULL,
    action_effectuee      VARCHAR(20) NOT NULL CHECK (action_effectuee IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CONNECT', 'DISCONNECT')),
    schema_cible          VARCHAR(50),
    table_cible           VARCHAR(100),
    adresse_ip            VARCHAR(45),
    date_acces           TIMESTAMP DEFAULT NOW(),
    succes_acces          BOOLEAN NOT NULL DEFAULT TRUE,
    message_acces         TEXT
);

-- Table d'audit de l'historique des statuts des contenurs
CREATE TABLE IF NOT EXISTS audit.historique_statut_conteneur (
    id_historique         BIGSERIAL PRIMARY KEY,
    numero_conteneur      CHAR(11) NOT NULL,
    statut_precedent      VARCHAR (20),
    statut_nouveau        VARCHAR (20) NOT NULL,
    date_changement       TIMESTAMP NOT NULL DEFAULT NOW(),
    port_code_locode      VARCHAR(5),
    operateur_logistique  VARCHAR(100),
    utilisateur_saisie    VARCHAR(50),
    date_enregistrement   TIMESTAMP DEFAULT NOW()
);