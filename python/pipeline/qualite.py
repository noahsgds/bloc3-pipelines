"""Contrôles qualité d'un événement de fin de match (critère 1.3 : exactitude des données).

Chaque événement est vérifié champ par champ. Le résultat est la liste des problèmes
trouvés, en français ; une liste vide veut dire « événement valide ».
Les règles sont celles du contrat de données de Sentinel (bloc 4).
"""

import uuid
from datetime import date

from pipeline.config import CATEGORIES, INTEGER_FIELDS, NUMERIC_RANGES, REQUIRED_FIELDS


def check_event(event: dict) -> list[str]:
    """Renvoie la liste des problèmes de l'événement (vide si tout est correct)."""
    problems = [f"{field} : champ obligatoire manquant" for field in REQUIRED_FIELDS
                if event.get(field) is None]  # fmt: skip

    for field in ("player_id", "match_id"):
        if event.get(field) is not None and not _is_uuid(event[field]):
            problems.append(f"{field} : identifiant invalide")

    if event.get("match_date") is not None and not _is_date(event["match_date"]):
        problems.append("match_date : date invalide (format AAAA-MM-JJ attendu)")

    for field, (low, high) in NUMERIC_RANGES.items():
        value = event.get(field)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            problems.append(f"{field} : nombre attendu")
        elif field in INTEGER_FIELDS and value != int(value):
            problems.append(f"{field} : nombre entier attendu")
        elif not low <= value <= high:
            problems.append(f"{field} : {value} hors des bornes [{low}, {high}]")

    for field, allowed in CATEGORIES.items():
        if event.get(field) is not None and event[field] not in allowed:
            problems.append(f"{field} : valeur inconnue « {event[field]} »")

    if event.get("accessibility_mode") is not None and not isinstance(
        event["accessibility_mode"], bool
    ):
        problems.append("accessibility_mode : vrai ou faux attendu")
    return problems


def _is_uuid(value) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False


def _is_date(value) -> bool:
    try:
        date.fromisoformat(str(value))
        return True
    except ValueError:
        return False
