"""Tests des contrôles qualité et de la simulation (sans base de données)."""

import json
import random

import pytest

from pipeline.etapes import minimize
from pipeline.qualite import check_event
from pipeline.simulation import make_event, write_batch


@pytest.fixture
def event():
    return make_event(random.Random(0))


def test_un_evenement_correct_passe(event):
    assert check_event(event) == []


def test_le_temps_de_reaction_peut_manquer(event):
    event["avg_reaction_ms"] = None
    assert check_event(event) == []


@pytest.mark.parametrize(
    "change, expected",
    [
        ({"accuracy": 1.7}, "accuracy : 1.7 hors des bornes"),
        ({"platform": "cloud"}, "platform : valeur inconnue"),
        ({"match_id": "pas-un-uuid"}, "match_id : identifiant invalide"),
        ({"match_date": "21/10/2026"}, "match_date : date invalide"),
        ({"rank_tier": 3.5}, "rank_tier : nombre entier attendu"),
        ({"kd_ratio": "2,4"}, "kd_ratio : nombre attendu"),
        ({"accessibility_mode": "oui"}, "accessibility_mode : vrai ou faux attendu"),
    ],
)
def test_chaque_defaut_est_detecte_avec_un_motif_clair(event, change, expected):
    problems = check_event({**event, **change})
    assert any(expected in p for p in problems), problems


def test_champ_obligatoire_manquant(event):
    del event["match_id"]
    assert check_event(event) == ["match_id : champ obligatoire manquant"]


def test_minimisation_retire_pseudo_et_ip(event):
    kept, unexpected = minimize(event)
    assert "pseudo" not in kept and "ip_address" not in kept
    assert unexpected == set()


def test_donnee_personnelle_inattendue_signalee_et_retiree(event):
    kept, unexpected = minimize({**event, "email": "joueur@exemple.fr"})
    assert unexpected == {"email"} and "email" not in kept


def test_un_serveur_simule_depose_un_fichier_realiste(tmp_path):
    path = write_batch(1_000, seed=1, landing_dir=tmp_path)
    events = json.loads(path.read_text())["evenements"]
    assert len(events) == 1_010  # 1 % de doublons
    defects = [e for e in events if check_event(e)]
    assert 5 < len(defects) < 60  # environ 2 % d'événements abîmés
    assert all("pseudo" in e for e in events)  # le pipeline devra les retirer


def test_un_serveur_peut_aussi_deposer_un_fichier_csv(tmp_path):
    from pipeline.etapes import read_events

    path = write_batch(200, seed=3, landing_dir=tmp_path, fmt="csv")
    events = read_events(path)
    assert path.suffix == ".csv" and len(events) == 202
    valid = [e for e in events if not check_event(e)]
    assert len(valid) > 180  # les types (nombres, booléens, vides) sont bien relus
