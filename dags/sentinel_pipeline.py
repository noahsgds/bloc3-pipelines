"""DAG Airflow du pipeline Sentinel : toutes les 5 minutes, du fichier de fin de match
jusqu'à l'API de détection de triche.

    simuler_serveurs -> ingerer -> controler -> transformer -> envoyer -> surveiller -> purger

- Un DAG est un enchaînement de tâches ; Airflow le lance selon un planning, affiche
  l'état de chaque tâche et réessaie automatiquement une tâche en échec.
- Toute la logique est dans python/pipeline/etapes.py (testée sans Airflow) ; ce fichier
  ne fait que décrire l'ordre, le planning et les règles de reprise.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from pipeline.db import connect
from pipeline.etapes import alert, run_step
from pipeline.simulation import write_batch


def on_failure(context) -> None:
    """Quand une tâche échoue malgré les nouvelles tentatives : alerte critique en base."""
    task_id = context["task_instance"].task_id
    with connect() as conn:
        alert(conn, context["run_id"], "critique", f"Étape « {task_id} » en échec après 3 essais")


@dag(
    dag_id="sentinel_pipeline",
    description="Fiches de fin de match -> contrôles -> fiches propres -> API Sentinel",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 10, 1),
    catchup=False,  # on ne rattrape pas les créneaux passés : seuls les fichiers présents comptent
    max_active_runs=1,  # une seule exécution à la fois : jamais deux lectures du même fichier
    default_args={
        "retries": 3,  # nouvelle tentative automatique (API indisponible, base redémarrée…)
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": on_failure,
    },
    tags=["sentinel", "bloc3"],
)
def sentinel_pipeline():
    @task
    def simuler_serveurs():
        """Démo uniquement : un serveur de jeu dépose un fichier de 500 fins de match."""
        if os.environ.get("SIMULATION", "true") == "true":
            write_batch(500)

    def etape(name: str):
        @task(task_id=name)
        def run(**context):
            return run_step(name, context["run_id"])

        return run()

    steps = [simuler_serveurs()] + [
        etape(name)
        for name in ["ingerer", "controler", "transformer", "envoyer", "surveiller", "purger"]
    ]
    for before, after in zip(steps, steps[1:]):
        before >> after


sentinel_pipeline()
