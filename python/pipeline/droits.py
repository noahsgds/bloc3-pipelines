"""Droits RGPD dans le pipeline : accès et effacement des données d'un joueur.

Le joueur est identifié par son identifiant pseudonyme (UUID), fourni par le système des
comptes après vérification de son identité. Chaque demande est enregistrée dans
pipeline.demandes_rgpd (preuve de traitement).

En ligne de commande :
    python -m pipeline.droits --acces <player_id>
    python -m pipeline.droits --effacer <player_id>
"""

import argparse
import json

from pipeline.db import connect, init_schema


def acces(conn, player_id: str) -> dict:
    """Toutes les données du pipeline concernant ce joueur."""
    fiches = conn.execute(
        "SELECT * FROM pipeline.fiches WHERE player_id = %s", (player_id,)
    ).fetchall()
    bruts = conn.execute(
        "SELECT id, fichier, recu_le, statut, contenu FROM pipeline.evenements_bruts "
        "WHERE contenu ->> 'player_id' = %s",
        (player_id,),
    ).fetchall()
    conn.execute(
        "INSERT INTO pipeline.demandes_rgpd (player_id, type, lignes) VALUES (%s, 'acces', %s)",
        (player_id, len(fiches) + len(bruts)),
    )
    return {"player_id": player_id, "fiches": fiches, "evenements_bruts": bruts}


def effacer(conn, player_id: str) -> int:
    """Supprime toutes les données du joueur (fiches, événements bruts et leur quarantaine).

    Note : si une sanction est contestée, la conservation des preuves relève de l'API
    Sentinel (bloc 4), qui garde les décisions ; le pipeline, lui, n'en a plus besoin.
    """
    n_fiches = conn.execute(
        "DELETE FROM pipeline.fiches WHERE player_id = %s", (player_id,)
    ).rowcount
    n_bruts = conn.execute(
        "DELETE FROM pipeline.evenements_bruts WHERE contenu ->> 'player_id' = %s", (player_id,)
    ).rowcount
    conn.execute(
        "INSERT INTO pipeline.demandes_rgpd (player_id, type, lignes) "
        "VALUES (%s, 'effacement', %s)",
        (player_id, n_fiches + n_bruts),
    )
    return n_fiches + n_bruts


def main() -> None:
    parser = argparse.ArgumentParser(description="Droits RGPD sur les données du pipeline.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--acces", metavar="PLAYER_ID")
    action.add_argument("--effacer", metavar="PLAYER_ID")
    args = parser.parse_args()
    with connect() as conn:
        init_schema(conn)
        if args.acces:
            print(json.dumps(acces(conn, args.acces), default=str, ensure_ascii=False, indent=2))
        else:
            print(f"{effacer(conn, args.effacer)} ligne(s) supprimée(s).")


if __name__ == "__main__":
    main()
