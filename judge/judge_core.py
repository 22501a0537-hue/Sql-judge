import os
import uuid
import mysql.connector
import psycopg2
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "user":     os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

# Use Neon connection string directly
POSTGRES_DSN = os.getenv("POSTGRES_URL")  # Full connection string from Neon


def run_mysql_judge(setup_sql: str, user_sql: str, expected_sql: str) -> dict:
    prefix = f"tmp_{uuid.uuid4().hex[:8]}"
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor()

        setup = setup_sql.replace("{{prefix}}", prefix)
        for stmt in _split_statements(setup):
            cur.execute(stmt)
        conn.commit()

        expected = expected_sql.replace("{{prefix}}", prefix)
        cur.execute(expected)
        expected_rows = sorted([list(r) for r in cur.fetchall()])

        user = user_sql.replace("{{prefix}}", prefix)
        cur.execute(user)
        user_rows = sorted([list(r) for r in cur.fetchall()])

        verdict = "AC" if user_rows == expected_rows else "WA"
        return {"verdict": verdict, "expected": expected_rows, "got": user_rows}

    except mysql.connector.Error as e:
        return {"verdict": "RE", "error": str(e)}
    finally:
        if conn:
            try:
                cur2 = conn.cursor()
                cur2.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name LIKE %s",
                    (os.getenv("MYSQL_DATABASE"), f"{prefix}%")
                )
                for (tbl,) in cur2.fetchall():
                    cur2.execute(f"DROP TABLE IF EXISTS `{tbl}`")
                conn.commit()
            except Exception:
                pass
            conn.close()


def run_postgres_judge(setup_sql: str, user_sql: str, expected_sql: str) -> dict:
    schema = f"tmp_{uuid.uuid4().hex[:8]}"
    conn = None
    try:
        # ✅ Connect directly using the DSN string — no need to split host/port/user
        conn = psycopg2.connect(POSTGRES_DSN, sslmode="require")
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")

        for stmt in _split_statements(setup_sql):
            cur.execute(stmt)

        cur.execute(expected_sql)
        expected_rows = sorted([list(r) for r in cur.fetchall()])

        cur.execute(user_sql)
        user_rows = sorted([list(r) for r in cur.fetchall()])

        verdict = "AC" if user_rows == expected_rows else "WA"
        conn.commit()
        return {"verdict": verdict, "expected": expected_rows, "got": user_rows}

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        return {"verdict": "RE", "error": str(e)}
    finally:
        if conn:
            try:
                conn.autocommit = True
                conn.cursor().execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            except Exception:
                pass
            conn.close()


def _split_statements(sql: str) -> list:
    return [s.strip() for s in sql.split(";") if s.strip()]
