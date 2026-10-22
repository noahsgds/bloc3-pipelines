"""Simulation des serveurs de jeu d'Arena Strike (données synthétiques).

À la fin de chaque match, un serveur de jeu dépose un fichier JSON dans le dossier
d'arrivée. Ce module fabrique ces fichiers pour la démonstration, avec des défauts
réalistes que le pipeline doit savoir gérer :
- environ 2 % d'événements invalides (valeur impossible, champ manquant, plateforme inconnue) ;
- quelques doublons (un serveur renvoie un événement déjà envoyé) ;
- des données personnelles inutiles (pseudo, adresse IP) que le pipeline doit supprimer.

En ligne de commande :  python -m pipeline.simulation --events 500
"""

import argparse
import csv
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pipeline.config import LANDING_DIR

DEFECT_RATE = 0.02
DUPLICATE_RATE = 0.01


def make_event(rng: random.Random) -> dict:
    """Un événement de fin de match plausible (un joueur sur un match)."""
    cheater = rng.random() < 0.03
    skill = rng.random()
    boost = rng.uniform(0.3, 1.0) if cheater else 0.0
    controller = rng.random() < 0.3
    return {
        "player_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "match_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "match_date": (date(2026, 10, 1) + timedelta(days=rng.randrange(30))).isoformat(),
        "pseudo": f"joueur{rng.randrange(100_000)}",  # donnée personnelle à supprimer
        "ip_address": f"10.{rng.randrange(256)}.{rng.randrange(256)}.{rng.randrange(256)}",
        "accuracy": round(min(0.95, 0.18 + 0.12 * skill + 0.15 * boost + rng.gauss(0, 0.04)), 3),
        "headshot_rate": round(max(0.0, min(1, 0.15 + 0.2 * skill + 0.3 * boost)), 3),
        "kd_ratio": round((0.8 + 0.6 * skill) * (1 + 0.8 * boost) * rng.lognormvariate(0, 0.3), 2),
        "kills_per_min": round(max(0.0, 0.5 + 0.4 * skill + 0.4 * boost + rng.gauss(0, 0.15)), 2),
        "damage_per_round": round(100 + 60 * skill + 50 * boost + rng.gauss(0, 20), 1),
        "avg_reaction_ms": None
        if rng.random() < 0.02
        else round(300 - 80 * skill - 130 * boost, 1),
        "kills_through_smoke_ratio": round(max(0.0, 0.02 + 0.1 * boost + rng.gauss(0, 0.02)), 3),
        "rank_tier": rng.randint(1, 8),
        "account_age_days": rng.randint(0, 2000),
        "hours_played_total": round(rng.uniform(1, 3000), 1),
        "reports_received": rng.choices(range(6), weights=[60, 20, 10, 5, 3, 2])[0],
        "input_device": "manette" if controller else "clavier_souris",
        "platform": "console" if controller else "pc",
        "age_band": rng.choice(["16-17", "18-24", "18-24", "25-34", "35+"]),
        "accessibility_mode": rng.random() < 0.04,
    }


def add_defect(event: dict, rng: random.Random) -> dict:
    """Abîme un événement d'une des façons rencontrées en production."""
    kind = rng.choice(["valeur_impossible", "champ_manquant", "plateforme_inconnue"])
    if kind == "valeur_impossible":
        event["accuracy"] = 1.7
    elif kind == "champ_manquant":
        del event["match_id"]
    else:
        event["platform"] = "cloud"
    return event


def write_batch(
    n_events: int, seed: int | None = None, landing_dir: Path = LANDING_DIR, fmt: str = "json"
) -> Path:
    """Écrit un fichier de ``n_events`` événements, comme un serveur de jeu.

    ``fmt`` vaut "json" (serveurs récents) ou "csv" (anciens serveurs) : le pipeline lit les deux.
    """
    rng = random.Random(seed)
    events = [make_event(rng) for _ in range(n_events)]
    events = [add_defect(e, rng) if rng.random() < DEFECT_RATE else e for e in events]
    events += [dict(e) for e in rng.sample(events, int(n_events * DUPLICATE_RATE))]
    now = datetime.now(UTC)
    payload = {
        "serveur": f"eu-west-{rng.randint(1, 3)}",
        "envoye_le": now.isoformat(),
        "evenements": events,
    }
    landing_dir.mkdir(parents=True, exist_ok=True)
    path = landing_dir / f"fin_de_match_{now:%Y%m%d_%H%M%S_%f}.{fmt}"
    if fmt == "csv":
        # Une ligne par événement ; un champ absent (événement abîmé) reste une cellule vide.
        columns = list(dict.fromkeys(key for event in events for key in event))
        with path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=columns, restval="")
            writer.writeheader()
            writer.writerows(events)
    else:
        path.write_text(json.dumps(payload, ensure_ascii=False))
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Simule un serveur de jeu qui envoie un fichier.")
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    args = parser.parse_args()
    path = write_batch(args.events, args.seed, fmt=args.format)
    print(f"Fichier déposé : {path}")


if __name__ == "__main__":
    main()
