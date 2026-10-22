"""Réglages du pipeline, rassemblés à un seul endroit.

Les valeurs propres à chaque environnement (adresse de la base, clé de l'API Sentinel)
viennent de variables d'environnement, jamais du code : aucun secret dans Git.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # racine du dépôt
SQL_DIR = ROOT / "sql"

# Dossier où les serveurs de jeu déposent leurs fichiers de fin de match (JSON ou CSV).
LANDING_DIR = Path(os.environ.get("LANDING_DIR", ROOT / "data" / "landing"))
# Une fois traité, un fichier est déplacé ici (on ne supprime jamais un fichier non traité).
PROCESSED_DIR = LANDING_DIR.parent / "traites"

DATABASE_URL = os.environ.get("PIPELINE_DATABASE_URL", "")
SENTINEL_API_URL = os.environ.get("SENTINEL_API_URL", "")  # vide = envoi désactivé
SENTINEL_API_KEY = os.environ.get("SENTINEL_API_KEY", "")
SEND_BATCH_SIZE = 1_000  # maximum accepté par l'API Sentinel par requête

# Seuil d'alerte : au-delà de 5 % de lignes rejetées, quelque chose ne va pas à la source.
MAX_REJECT_RATE = 0.05
# Seuil d'alerte : une exécution plus longue que 2 minutes risque de chevaucher la suivante
# (une toutes les 5 minutes). Aujourd'hui, une journée entière passe en 5 secondes.
MAX_RUN_SECONDS = 120

# --- Contrat de données (identique au bloc 4) ---------------------------------------------
NUMERIC_RANGES = {
    "accuracy": (0.0, 1.0),
    "headshot_rate": (0.0, 1.0),
    "kd_ratio": (0.0, 50.0),
    "kills_per_min": (0.0, 10.0),
    "damage_per_round": (0.0, 1000.0),
    "avg_reaction_ms": (50.0, 2000.0),
    "kills_through_smoke_ratio": (0.0, 1.0),
    "rank_tier": (1, 8),
    "account_age_days": (0, 5000),
    "hours_played_total": (0.0, 20000.0),
    "reports_received": (0, 50),
}
INTEGER_FIELDS = {"rank_tier", "account_age_days", "reports_received"}
OPTIONAL_FIELDS = {"avg_reaction_ms"}
CATEGORIES = {
    "input_device": {"clavier_souris", "manette"},
    "platform": {"pc", "console"},
    "age_band": {"16-17", "18-24", "25-34", "35+"},
}
REQUIRED_FIELDS = (
    ["player_id", "match_id", "match_date", "accessibility_mode"]
    + [f for f in NUMERIC_RANGES if f not in OPTIONAL_FIELDS]
    + list(CATEGORIES)
)

# Champs envoyés à l'API Sentinel (les variables d'audit n'en font pas partie).
API_FIELDS = ["player_id", "match_id", *NUMERIC_RANGES, "input_device", "platform"]

# Minimisation (RGPD) : données personnelles envoyées par les serveurs de jeu mais inutiles
# à la détection de triche. Elles sont supprimées dès l'ingestion, jamais stockées.
REMOVED_FIELDS = {"pseudo", "ip_address"}
# Tout autre champ de cette liste dans un fichier est anormal : alerte "violation possible".
PERSONAL_FIELDS = {"email", "nom", "prenom", "telephone", "adresse", "chat_messages"}
