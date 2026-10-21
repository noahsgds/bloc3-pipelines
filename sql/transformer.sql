-- Transformation : événements bruts validés (JSON) -> fiches propres (colonnes typées).
--
-- On prend TOUS les événements validés pas encore chargés, même s'ils viennent d'une
-- exécution précédente interrompue : c'est la reprise automatique après erreur.
-- ON CONFLICT DO NOTHING : une fiche (joueur, match) reçue deux fois n'est chargée qu'une
-- fois. Les doublons sont comptés par différence dans le code Python.

INSERT INTO pipeline.fiches (
    player_id, match_id, match_date,
    accuracy, headshot_rate, kd_ratio, kills_per_min, damage_per_round,
    avg_reaction_ms, kills_through_smoke_ratio,
    rank_tier, account_age_days, hours_played_total, reports_received,
    input_device, platform, age_band, accessibility_mode,
    brut_id, fichier, run_id
)
SELECT
    (contenu ->> 'player_id')::uuid,
    (contenu ->> 'match_id')::uuid,
    (contenu ->> 'match_date')::date,
    (contenu ->> 'accuracy')::real,
    (contenu ->> 'headshot_rate')::real,
    (contenu ->> 'kd_ratio')::real,
    (contenu ->> 'kills_per_min')::real,
    (contenu ->> 'damage_per_round')::real,
    (contenu ->> 'avg_reaction_ms')::real,
    (contenu ->> 'kills_through_smoke_ratio')::real,
    (contenu ->> 'rank_tier')::numeric::smallint,
    (contenu ->> 'account_age_days')::numeric::integer,
    (contenu ->> 'hours_played_total')::real,
    (contenu ->> 'reports_received')::numeric::smallint,
    contenu ->> 'input_device',
    contenu ->> 'platform',
    contenu ->> 'age_band',
    (contenu ->> 'accessibility_mode')::boolean,
    id, fichier, run_id
FROM pipeline.evenements_bruts
WHERE statut = 'valide'
ORDER BY id
ON CONFLICT (player_id, match_id) DO NOTHING;

UPDATE pipeline.evenements_bruts SET statut = 'charge' WHERE statut = 'valide';
