-- Tables du pipeline Sentinel (base "pipeline").
-- Exécuté à chaque démarrage du pipeline : "IF NOT EXISTS" rend le script rejouable sans risque.

CREATE SCHEMA IF NOT EXISTS pipeline;

-- 1. Registre des fichiers reçus : un fichier n'est jamais traité deux fois (reprise après
--    erreur sans doublon). Sert aussi à tracer l'origine de chaque ligne.
CREATE TABLE IF NOT EXISTS pipeline.fichiers (
    nom            TEXT PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    recu_le        TIMESTAMPTZ NOT NULL DEFAULT now(),
    nb_evenements  INTEGER     NOT NULL,
    statut         TEXT        NOT NULL CHECK (statut IN ('traite', 'illisible'))
);

-- 2. Événements bruts, tels que reçus (format JSON), APRÈS suppression des données
--    personnelles non nécessaires (pseudo, adresse IP). Conservés 30 jours.
--    Statut : a_controler -> valide ou rejete -> charge (copié dans les fiches).
CREATE TABLE IF NOT EXISTS pipeline.evenements_bruts (
    id        BIGSERIAL PRIMARY KEY,
    fichier   TEXT        NOT NULL REFERENCES pipeline.fichiers (nom),
    run_id    TEXT        NOT NULL,
    recu_le   TIMESTAMPTZ NOT NULL DEFAULT now(),
    contenu   JSONB       NOT NULL,
    statut    TEXT        NOT NULL DEFAULT 'a_controler'
              CHECK (statut IN ('a_controler', 'valide', 'rejete', 'charge'))
);
CREATE INDEX IF NOT EXISTS idx_bruts_statut ON pipeline.evenements_bruts (statut);

-- 3. Quarantaine : lignes refusées par les contrôles qualité, avec le motif. Elles ne
--    bloquent pas le pipeline et peuvent être corrigées puis rejouées. Conservées 30 jours.
CREATE TABLE IF NOT EXISTS pipeline.quarantaine (
    id        BIGSERIAL PRIMARY KEY,
    brut_id   BIGINT REFERENCES pipeline.evenements_bruts (id) ON DELETE CASCADE,
    fichier   TEXT        NOT NULL,
    run_id    TEXT        NOT NULL,
    motifs    TEXT[]      NOT NULL,
    cree_le   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 4. Fiches propres, au format exact attendu par Sentinel (contrat du bloc 4).
--    Une fiche = un joueur sur un match. Conservées 6 mois.
CREATE TABLE IF NOT EXISTS pipeline.fiches (
    player_id                  UUID        NOT NULL,
    match_id                   UUID        NOT NULL,
    match_date                 DATE        NOT NULL,
    accuracy                   REAL        NOT NULL,
    headshot_rate              REAL        NOT NULL,
    kd_ratio                   REAL        NOT NULL,
    kills_per_min              REAL        NOT NULL,
    damage_per_round           REAL        NOT NULL,
    avg_reaction_ms            REAL,                    -- peut manquer (télémétrie perdue)
    kills_through_smoke_ratio  REAL        NOT NULL,
    rank_tier                  SMALLINT    NOT NULL,
    account_age_days           INTEGER     NOT NULL,
    hours_played_total         REAL        NOT NULL,
    reports_received           SMALLINT    NOT NULL,
    input_device               TEXT        NOT NULL,
    platform                   TEXT        NOT NULL,
    age_band                   TEXT        NOT NULL,    -- audit d'équité uniquement
    accessibility_mode         BOOLEAN     NOT NULL,    -- audit d'équité uniquement
    -- Traçabilité : d'où vient la fiche, quand et par quelle exécution elle a été chargée.
    brut_id                    BIGINT      NOT NULL,
    fichier                    TEXT        NOT NULL,
    run_id                     TEXT        NOT NULL,
    charge_le                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    envoye_le                  TIMESTAMPTZ,             -- envoi à l'API Sentinel
    PRIMARY KEY (player_id, match_id)
);
CREATE INDEX IF NOT EXISTS idx_fiches_a_envoyer ON pipeline.fiches (envoye_le) WHERE envoye_le IS NULL;

-- 5. Mesures de chaque étape de chaque exécution (surveillance).
CREATE TABLE IF NOT EXISTS pipeline.mesures (
    run_id    TEXT        NOT NULL,
    etape     TEXT        NOT NULL,
    debut     TIMESTAMPTZ NOT NULL,
    duree_s   REAL        NOT NULL,
    lignes    INTEGER     NOT NULL,
    details   JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, etape)
);

-- 6. Alertes (taux de rejet élevé, fichier illisible, donnée personnelle inattendue, échec).
CREATE TABLE IF NOT EXISTS pipeline.alertes (
    id        BIGSERIAL PRIMARY KEY,
    cree_le   TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id    TEXT,
    niveau    TEXT        NOT NULL CHECK (niveau IN ('avertissement', 'critique')),
    message   TEXT        NOT NULL
);

-- 7. Registre des demandes RGPD (accès, effacement) : preuve de traitement.
CREATE TABLE IF NOT EXISTS pipeline.demandes_rgpd (
    id         BIGSERIAL PRIMARY KEY,
    cree_le    TIMESTAMPTZ NOT NULL DEFAULT now(),
    player_id  UUID        NOT NULL,
    type       TEXT        NOT NULL CHECK (type IN ('acces', 'effacement')),
    lignes     INTEGER     NOT NULL
);

-- Vue de synthèse : une ligne par exécution, pour Grafana (bloc 2) et le suivi.
CREATE OR REPLACE VIEW pipeline.executions AS
SELECT
    run_id,
    min(debut)                                                        AS debut,
    sum(duree_s)                                                      AS duree_s,
    max(lignes) FILTER (WHERE etape = 'ingerer')                      AS lignes_lues,
    max(lignes) FILTER (WHERE etape = 'controler')                    AS lignes_valides,
    max((details ->> 'rejetees')::int) FILTER (WHERE etape = 'controler') AS lignes_rejetees,
    max(lignes) FILTER (WHERE etape = 'transformer')                  AS fiches_chargees,
    max(lignes) FILTER (WHERE etape = 'envoyer')                      AS fiches_envoyees
FROM pipeline.mesures
GROUP BY run_id;
