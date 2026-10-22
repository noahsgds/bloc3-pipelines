"""Les étapes du pipeline Sentinel. Airflow les enchaîne (dags/sentinel_pipeline.py).

    ingerer -> controler -> transformer -> envoyer -> surveiller -> purger

Chaque étape :
- reprend ce qui reste à faire, même si une exécution précédente s'est arrêtée en route
  (reprise automatique après erreur) ;
- valide ses écritures d'un bloc (une étape interrompue ne laisse rien à moitié écrit) ;
- renvoie le nombre de lignes traitées et des détails, enregistrés dans pipeline.mesures.
"""

import csv
import json
import logging
import shutil
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from psycopg.types.json import Jsonb

from pipeline import config
from pipeline.db import connect, init_schema, run_sql_file
from pipeline.qualite import check_event

log = logging.getLogger("sentinel.pipeline")


def alert(conn, run_id: str, level: str, message: str) -> None:
    """Enregistre une alerte dans pipeline.alertes et l'écrit dans le journal d'Airflow."""
    conn.execute(
        "INSERT INTO pipeline.alertes (run_id, niveau, message) VALUES (%s, %s, %s)",
        (run_id, level, message),
    )
    log.warning("ALERTE %s : %s", level, message)


# --- 1. Ingestion -------------------------------------------------------------------------


def minimize(event) -> tuple[dict, set]:
    """Retire les données personnelles inutiles. Renvoie (événement, champs anormaux trouvés)."""
    if not isinstance(event, dict):
        return {"contenu_invalide": str(event)[:200]}, set()
    unexpected = set(event) & config.PERSONAL_FIELDS
    kept = {k: v for k, v in event.items() if k not in config.REMOVED_FIELDS | unexpected}
    return kept, unexpected


def _from_csv(value: str):
    """Convertit une cellule CSV (toujours du texte) dans le bon type, comme en JSON."""
    if value == "":
        return None  # cellule vide = valeur absente
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    for convert in (int, float):
        try:
            return convert(value)
        except ValueError:
            pass
    return value


def read_events(path: Path) -> list:
    """Lit les événements d'un fichier JSON ({"evenements": [...]}) ou CSV.

    En CSV, chaque ligne est un événement et la première ligne donne les noms des champs.
    Un fichier mal formé lève une erreur : il sera mis de côté avec une alerte.
    """
    if path.suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as file:
            return [{k: _from_csv(v) for k, v in row.items()} for row in csv.DictReader(file)]
    events = json.loads(path.read_text())["evenements"]
    if not isinstance(events, list):
        raise ValueError("'evenements' n'est pas une liste")
    return events


def ingerer(conn, run_id: str, landing_dir: Path | None = None) -> dict:
    """Lit les nouveaux fichiers (JSON ou CSV) et copie leurs événements (minimisés) en base.

    Un fichier déjà enregistré n'est jamais relu (pas de doublon après une reprise).
    Un fichier illisible est mis de côté avec une alerte ; les autres continuent.
    """
    landing_dir = landing_dir or config.LANDING_DIR
    processed_dir = landing_dir.parent / "traites"
    processed_dir.mkdir(parents=True, exist_ok=True)
    n_files = n_events = 0
    files = sorted([*landing_dir.glob("*.json"), *landing_dir.glob("*.csv")], key=lambda p: p.name)
    for path in files:
        known = conn.execute(
            "SELECT 1 FROM pipeline.fichiers WHERE nom = %s", (path.name,)
        ).fetchone()
        if known:  # déjà traité lors d'une exécution interrompue avant le déplacement
            shutil.move(path, processed_dir / path.name)
            continue
        try:
            events = read_events(path)
        except (ValueError, KeyError, TypeError, csv.Error, UnicodeDecodeError) as error:
            conn.execute(
                "INSERT INTO pipeline.fichiers (nom, run_id, nb_evenements, statut) "
                "VALUES (%s, %s, 0, 'illisible')",
                (path.name, run_id),
            )
            alert(
                conn, run_id, "critique", f"Fichier illisible mis de côté : {path.name} ({error})"
            )
            conn.commit()
            shutil.move(path, processed_dir / path.name)
            continue

        cleaned, unexpected = [], set()
        for event in events:
            kept, found = minimize(event)
            cleaned.append(kept)
            unexpected |= found
        conn.execute(
            "INSERT INTO pipeline.fichiers (nom, run_id, nb_evenements, statut) "
            "VALUES (%s, %s, %s, 'traite')",
            (path.name, run_id, len(cleaned)),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO pipeline.evenements_bruts (fichier, run_id, contenu) "
                "VALUES (%s, %s, %s)",
                [(path.name, run_id, Jsonb(e)) for e in cleaned],
            )
        if unexpected:
            message = (
                f"Données personnelles inattendues dans {path.name} : {sorted(unexpected)}. "
                "Supprimées avant stockage ; prévenir le DPO (violation possible à la source)."
            )
            alert(conn, run_id, "critique", message)
        conn.commit()  # le fichier et ses événements sont enregistrés ensemble, ou pas du tout
        shutil.move(path, processed_dir / path.name)
        n_files += 1
        n_events += len(cleaned)
    return {"lignes": n_events, "details": {"fichiers": n_files}}


# --- 2. Contrôles qualité -----------------------------------------------------------------


def controler(conn, run_id: str) -> dict:
    """Vérifie chaque événement en attente ; les invalides vont en quarantaine avec le motif."""
    rows = conn.execute(
        "SELECT id, fichier, contenu FROM pipeline.evenements_bruts WHERE statut = 'a_controler'"
    ).fetchall()
    valid, rejected = [], []
    for row in rows:
        problems = check_event(row["contenu"])
        if problems:
            rejected.append((row["id"], row["fichier"], run_id, problems))
        else:
            valid.append((row["id"],))
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE pipeline.evenements_bruts SET statut = 'valide' WHERE id = %s", valid
        )
        cur.executemany(
            "UPDATE pipeline.evenements_bruts SET statut = 'rejete' WHERE id = %s",
            [(r[0],) for r in rejected],
        )
        cur.executemany(
            "INSERT INTO pipeline.quarantaine (brut_id, fichier, run_id, motifs) "
            "VALUES (%s, %s, %s, %s)",
            rejected,
        )
    conn.commit()
    return {"lignes": len(valid), "details": {"controlees": len(rows), "rejetees": len(rejected)}}


# --- 3. Transformation --------------------------------------------------------------------


def transformer(conn, run_id: str) -> dict:
    """Copie les événements validés dans la table des fiches (SQL : sql/transformer.sql)."""
    n_valid = conn.execute(
        "SELECT count(*) AS n FROM pipeline.evenements_bruts WHERE statut = 'valide'"
    ).fetchone()["n"]
    before = conn.execute("SELECT count(*) AS n FROM pipeline.fiches").fetchone()["n"]
    run_sql_file(conn, "transformer.sql")
    loaded = conn.execute("SELECT count(*) AS n FROM pipeline.fiches").fetchone()["n"] - before
    conn.commit()
    return {"lignes": loaded, "details": {"doublons": n_valid - loaded}}


# --- 4. Envoi à l'API Sentinel (bloc 4) ---------------------------------------------------


def post_batch(fiches: list[dict]) -> None:
    """Envoie un lot à POST /predict. Lève une erreur si l'API refuse ou ne répond pas."""
    body = json.dumps({"fiches": fiches}, default=str).encode()
    request = urllib.request.Request(
        config.SENTINEL_API_URL.rstrip("/") + "/predict",
        data=body,
        headers={"Content-Type": "application/json", "X-API-Key": config.SENTINEL_API_KEY},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"API Sentinel : statut {response.status}")


def envoyer(conn, run_id: str, send=post_batch) -> dict:
    """Envoie les fiches pas encore transmises, par lots de 1 000.

    Chaque lot envoyé est marqué aussitôt : si l'API tombe au milieu, la prochaine
    tentative (Airflow réessaie automatiquement) ne renvoie que ce qui manque.
    """
    if not config.SENTINEL_API_URL:
        log.info("SENTINEL_API_URL non défini : envoi désactivé")
        return {"lignes": 0, "details": {"envoi": "desactive"}}
    columns = ", ".join(config.API_FIELDS)
    sent = 0
    while True:
        batch = conn.execute(
            f"SELECT {columns} FROM pipeline.fiches WHERE envoye_le IS NULL "  # noqa: S608
            "ORDER BY charge_le LIMIT %s",
            (config.SEND_BATCH_SIZE,),
        ).fetchall()
        if not batch:
            break
        send(batch)
        conn.execute(
            "UPDATE pipeline.fiches SET envoye_le = now() WHERE (player_id, match_id) IN "
            "(SELECT unnest(%s::uuid[]), unnest(%s::uuid[]))",
            ([b["player_id"] for b in batch], [b["match_id"] for b in batch]),
        )
        conn.commit()
        sent += len(batch)
    return {"lignes": sent, "details": {}}


# --- 5. Surveillance ----------------------------------------------------------------------


def surveiller(conn, run_id: str) -> dict:
    """Contrôle l'exécution : alerte si plus de 5 % de rejets ou si elle a été trop lente."""
    m = conn.execute(
        "SELECT lignes_lues, lignes_valides, lignes_rejetees, duree_s FROM pipeline.executions "
        "WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    checked = (m["lignes_valides"] or 0) + (m["lignes_rejetees"] or 0) if m else 0
    rate = (m["lignes_rejetees"] or 0) / checked if checked else 0.0
    if rate > config.MAX_REJECT_RATE:
        alert(conn, run_id, "avertissement",
              f"{rate:.1%} de lignes rejetées (maximum {config.MAX_REJECT_RATE:.0%}) : "
              "vérifier les serveurs de jeu.")  # fmt: skip
    duration = (m["duree_s"] or 0) if m else 0  # durée cumulée des étapes déjà passées
    if duration > config.MAX_RUN_SECONDS:
        alert(conn, run_id, "avertissement",
              f"Exécution lente : {duration:.0f} s (maximum {config.MAX_RUN_SECONDS} s), "
              "risque de chevauchement avec l'exécution suivante.")  # fmt: skip
    conn.commit()
    return {"lignes": checked, "details": {"taux_rejet": round(rate, 4), "duree_s": duration}}


# --- 6. Purge RGPD ------------------------------------------------------------------------


def purger(conn, run_id: str) -> dict:
    """Supprime les données au-delà de leur durée de conservation (sql/purger.sql)."""
    deleted = run_sql_file(conn, "purger.sql")
    conn.commit()
    return {"lignes": deleted, "details": {}}


# --- Exécution d'une étape avec mesure ----------------------------------------------------

STEPS = {
    "ingerer": ingerer,
    "controler": controler,
    "transformer": transformer,
    "envoyer": envoyer,
    "surveiller": surveiller,
    "purger": purger,
}


def run_step(name: str, run_id: str, url: str = "") -> dict:
    """Exécute une étape et enregistre sa durée et son volume dans pipeline.mesures."""
    start = datetime.now(UTC)
    t0 = time.perf_counter()
    with connect(url) as conn:
        init_schema(conn)
        result = STEPS[name](conn, run_id)
        duration = round(time.perf_counter() - t0, 3)
        conn.execute(
            "INSERT INTO pipeline.mesures (run_id, etape, debut, duree_s, lignes, details) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (run_id, etape) DO UPDATE SET "
            "debut = EXCLUDED.debut, duree_s = EXCLUDED.duree_s, lignes = EXCLUDED.lignes, "
            "details = EXCLUDED.details",
            (run_id, name, start, duration, result["lignes"], Jsonb(result["details"])),
        )
    log.info("%s : %s lignes en %.2f s %s", name, result["lignes"], duration, result["details"])
    return result
