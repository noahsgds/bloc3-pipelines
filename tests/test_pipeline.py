"""Tests du pipeline complet sur une vraie base Postgres.

Ils s'exécutent si la variable TEST_DATABASE_URL pointe vers une base de test (c'est le
cas dans la CI GitHub) ; sinon ils sont ignorés. La base est vidée avant chaque test.
"""

import json
import os

import pytest

from pipeline import config, droits, etapes
from pipeline.db import connect, init_schema
from pipeline.simulation import write_batch

URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL non définie")


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SENTINEL_API_URL", "")
    with connect(URL) as c:
        init_schema(c)
        c.execute(
            "TRUNCATE pipeline.fiches, pipeline.quarantaine, pipeline.evenements_bruts, "
            "pipeline.fichiers, pipeline.mesures, pipeline.alertes, pipeline.demandes_rgpd"
        )
        c.commit()
        yield c


@pytest.fixture
def landing(tmp_path):
    folder = tmp_path / "landing"
    folder.mkdir()
    return folder


def count(conn, sql: str) -> int:
    return conn.execute(sql).fetchone()["n"]


def run_all(conn, run_id, landing):
    etapes.ingerer(conn, run_id, landing)
    etapes.controler(conn, run_id)
    etapes.transformer(conn, run_id)


def test_pipeline_de_bout_en_bout(conn, landing):
    write_batch(2_000, seed=1, landing_dir=landing)
    read = etapes.ingerer(conn, "r1", landing)
    checked = etapes.controler(conn, "r1")
    loaded = etapes.transformer(conn, "r1")
    assert read["lignes"] == 2_020
    assert checked["lignes"] + checked["details"]["rejetees"] == 2_020
    assert loaded["lignes"] + loaded["details"]["doublons"] == checked["lignes"]
    assert count(conn, "SELECT count(*) n FROM pipeline.fiches") == loaded["lignes"]
    assert list(landing.glob("*.json")) == []  # fichier déplacé dans "traites"


def test_minimisation_aucun_pseudo_ni_ip_stocke(conn, landing):
    write_batch(200, seed=2, landing_dir=landing)
    run_all(conn, "r1", landing)
    n = count(conn, "SELECT count(*) n FROM pipeline.evenements_bruts "
                    "WHERE contenu ? 'pseudo' OR contenu ? 'ip_address'")  # fmt: skip
    assert n == 0


def test_fichier_illisible_mis_de_cote_et_le_reste_continue(conn, landing):
    (landing / "a_casse.json").write_text("{pas du json")
    write_batch(100, seed=3, landing_dir=landing)
    result = etapes.ingerer(conn, "r1", landing)
    assert result["details"]["fichiers"] == 1
    alerts = conn.execute("SELECT niveau, message FROM pipeline.alertes").fetchall()
    assert alerts[0]["niveau"] == "critique" and "illisible" in alerts[0]["message"]


def test_donnee_personnelle_inattendue_declenche_une_alerte(conn, landing):
    event = {"player_id": "x", "email": "joueur@exemple.fr"}
    (landing / "perso.json").write_text(json.dumps({"evenements": [event]}))
    etapes.ingerer(conn, "r1", landing)
    assert "email" in conn.execute("SELECT message FROM pipeline.alertes").fetchone()["message"]
    assert (
        count(conn, "SELECT count(*) n FROM pipeline.evenements_bruts WHERE contenu ? 'email'") == 0
    )


def test_relancer_ne_cree_aucun_doublon(conn, landing):
    write_batch(300, seed=4, landing_dir=landing)
    run_all(conn, "r1", landing)
    before = count(conn, "SELECT count(*) n FROM pipeline.fiches")
    path = write_batch(300, seed=4, landing_dir=landing)  # même contenu, nouveau fichier
    run_all(conn, "r2", landing)
    assert count(conn, "SELECT count(*) n FROM pipeline.fiches") == before
    assert not path.exists()


def test_reprise_apres_une_execution_interrompue(conn, landing):
    """Une exécution s'arrête après le contrôle : la suivante termine le travail."""
    write_batch(300, seed=5, landing_dir=landing)
    etapes.ingerer(conn, "r1", landing)
    etapes.controler(conn, "r1")  # ... puis panne avant la transformation
    loaded = etapes.transformer(conn, "r2")  # exécution suivante
    assert loaded["lignes"] > 0
    assert (
        count(conn, "SELECT count(*) n FROM pipeline.evenements_bruts WHERE statut = 'valide'") == 0
    )


def test_envoi_reprend_la_ou_il_s_est_arrete(conn, landing, monkeypatch):
    monkeypatch.setattr(config, "SENTINEL_API_URL", "http://sentinel")
    monkeypatch.setattr(config, "SEND_BATCH_SIZE", 100)
    write_batch(250, seed=6, landing_dir=landing)
    run_all(conn, "r1", landing)
    total = count(conn, "SELECT count(*) n FROM pipeline.fiches")
    calls = []

    def api_that_fails_on_second_batch(batch):
        calls.append(len(batch))
        if len(calls) == 2:
            raise ConnectionError("API indisponible")

    with pytest.raises(ConnectionError):
        etapes.envoyer(conn, "r1", send=api_that_fails_on_second_batch)
    assert (
        count(conn, "SELECT count(*) n FROM pipeline.fiches WHERE envoye_le IS NULL") == total - 100
    )
    result = etapes.envoyer(conn, "r1", send=lambda batch: None)  # nouvelle tentative
    assert result["lignes"] == total - 100  # seules les fiches manquantes sont renvoyées


def test_envoi_desactive_sans_adresse_d_api(conn):
    assert etapes.envoyer(conn, "r1")["details"] == {"envoi": "desactive"}


def test_alerte_si_trop_de_lignes_rejetees(conn, landing, monkeypatch):
    monkeypatch.setattr(config, "LANDING_DIR", landing)
    events = [{"player_id": "x"}] * 10  # 10 lignes invalides sur 10
    (landing / "mauvais.json").write_text(json.dumps({"evenements": events}))
    etapes.run_step("ingerer", "r1", URL)
    etapes.run_step("controler", "r1", URL)
    result = etapes.run_step("surveiller", "r1", URL)
    assert result["details"]["taux_rejet"] == 1.0
    assert "rejetées" in conn.execute("SELECT message FROM pipeline.alertes").fetchone()["message"]


def test_mesures_enregistrees_pour_chaque_etape(conn, landing):
    write_batch(100, seed=7, landing_dir=landing)
    for step in ["controler", "transformer", "purger"]:
        etapes.run_step(step, "r1", URL)
    steps = {r["etape"] for r in conn.execute("SELECT etape FROM pipeline.mesures").fetchall()}
    assert steps == {"controler", "transformer", "purger"}


def test_purge_des_donnees_trop_anciennes(conn, landing):
    write_batch(100, seed=8, landing_dir=landing)
    run_all(conn, "r1", landing)
    conn.execute("UPDATE pipeline.fiches SET charge_le = now() - interval '7 months'")
    conn.execute("UPDATE pipeline.evenements_bruts SET recu_le = now() - interval '31 days'")
    etapes.purger(conn, "r2")
    assert count(conn, "SELECT count(*) n FROM pipeline.fiches") == 0
    assert count(conn, "SELECT count(*) n FROM pipeline.evenements_bruts") == 0
    assert count(conn, "SELECT count(*) n FROM pipeline.quarantaine") == 0


def test_droits_d_acces_et_d_effacement(conn, landing):
    write_batch(50, seed=9, landing_dir=landing)
    run_all(conn, "r1", landing)
    player = str(
        conn.execute("SELECT player_id FROM pipeline.fiches LIMIT 1").fetchone()["player_id"]
    )
    data = droits.acces(conn, player)
    assert len(data["fiches"]) >= 1 and len(data["evenements_bruts"]) >= 1
    assert droits.effacer(conn, player) >= 2
    assert droits.acces(conn, player)["fiches"] == []
    types = [
        r["type"]
        for r in conn.execute("SELECT type FROM pipeline.demandes_rgpd ORDER BY id").fetchall()
    ]
    assert types == ["acces", "effacement", "acces"]
