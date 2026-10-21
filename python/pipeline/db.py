"""Connexion à la base Postgres du pipeline.

Toutes les requêtes passent des valeurs en paramètres (%s), jamais collées dans le texte
SQL : cela empêche les injections SQL.
"""

from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from pipeline.config import DATABASE_URL, SQL_DIR


@contextmanager
def connect(url: str = ""):
    """Ouvre une connexion ; tout est validé (commit) à la fin, ou annulé en cas d'erreur.

    C'est ce qui garantit qu'une étape interrompue ne laisse rien à moitié écrit.
    """
    with psycopg.connect(url or DATABASE_URL, row_factory=dict_row) as conn:
        yield conn


def run_sql_file(conn, name: str, params: dict | None = None) -> int:
    """Exécute un fichier du dossier sql/ et renvoie le nombre de lignes touchées."""
    total = 0
    with conn.cursor() as cur:
        statements = [s for s in (SQL_DIR / name).read_text().split(";") if _has_code(s)]
        for statement in statements:
            cur.execute(statement, params or {})
            total += max(cur.rowcount, 0)
    return total


def _has_code(statement: str) -> bool:
    """Vrai si l'instruction contient autre chose que des commentaires."""
    lines = [line.strip() for line in statement.splitlines()]
    return any(line and not line.startswith("--") for line in lines)


def init_schema(conn) -> None:
    """Crée les tables si elles n'existent pas (rejouable sans risque)."""
    run_sql_file(conn, "schema.sql")
