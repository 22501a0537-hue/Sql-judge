import os
import uuid
import mysql.connector
import psycopg2
import threading
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

mysql_lock = threading.Lock()


def run_mysql_judge(setup_sql: str, user_sql: str, expected_sql: str) -> dict:
    """
    Execute setup → expected_sql → user_sql on a shared MySQL database.
    Returns verdict, expected rows, user rows, AND column headers from both queries.
    Uses a threading lock to prevent table collisions across concurrent requests.
    """
    conn = None
    created_tables = []

    with mysql_lock:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cur = conn.cursor()

            # ── 1. Setup: create tables & insert data ──
            if setup_sql and setup_sql.strip():
                statements = _split_statements(setup_sql)
                for stmt in statements:
                    upper = stmt.strip().upper()
                    if upper.startswith("CREATE TABLE"):
                        parts = stmt.split()
                        tbl_idx = None
                        for pi, p in enumerate(parts):
                            if p.upper() == "TABLE":
                                tbl_idx = pi + 1
                                break
                        if tbl_idx and tbl_idx < len(parts):
                            raw = parts[tbl_idx]
                            tbl_name = raw.strip("`'\"(").rstrip(")`'\"")
                            # Handle IF NOT EXISTS
                            if tbl_name.upper() in ("IF", "NOT", "EXISTS"):
                                for j in range(tbl_idx + 1, len(parts)):
                                    candidate = parts[j].strip("`'\"(").rstrip(")`'\"")
                                    if candidate.upper() not in ("IF", "NOT", "EXISTS"):
                                        tbl_name = candidate
                                        break
                            cur.execute(f"DROP TABLE IF EXISTS `{tbl_name}`")
                            created_tables.append(tbl_name)
                    cur.execute(stmt)
                conn.commit()

            # ── 2. Run expected (reference) query ──
            cur.execute(expected_sql)
            expected_columns = [desc[0] for desc in cur.description] if cur.description else []
            expected_rows = [list(r) for r in cur.fetchall()]

            # ── 3. Run user query ──
            cur.execute(user_sql)
            user_columns = [desc[0] for desc in cur.description] if cur.description else []
            user_rows = [list(r) for r in cur.fetchall()]

            # ── 4. Compare (sort for order-insensitive check) ──
            verdict = "AC" if sorted(user_rows) == sorted(expected_rows) else "WA"

            return {
                "verdict": verdict,
                "expected": expected_rows,
                "got": user_rows,
                "expected_columns": expected_columns,
                "columns": user_columns,
            }

        except mysql.connector.Error as e:
            return {"verdict": "RE", "error": str(e)}
        finally:
            if conn:
                try:
                    cur2 = conn.cursor()
                    for tbl in created_tables:
                        cur2.execute(f"DROP TABLE IF EXISTS `{tbl}`")
                    conn.commit()
                except Exception:
                    pass
                conn.close()


def run_postgres_judge(setup_sql: str, user_sql: str, expected_sql: str) -> dict:
    """
    Execute setup → expected_sql → user_sql inside an isolated Postgres schema.
    Returns verdict, expected rows, user rows, AND column headers.
    """
    schema = f"tmp_{uuid.uuid4().hex[:8]}"
    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_DSN, sslmode="require")
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")

        for stmt in _split_statements(setup_sql):
            cur.execute(stmt)

        # Expected
        cur.execute(expected_sql)
        expected_columns = [desc[0] for desc in cur.description] if cur.description else []
        expected_rows = [list(r) for r in cur.fetchall()]

        # User
        cur.execute(user_sql)
        user_columns = [desc[0] for desc in cur.description] if cur.description else []
        user_rows = [list(r) for r in cur.fetchall()]

        verdict = "AC" if sorted(user_rows) == sorted(expected_rows) else "WA"
        conn.commit()
        return {
            "verdict": verdict,
            "expected": expected_rows,
            "got": user_rows,
            "expected_columns": expected_columns,
            "columns": user_columns,
        }

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
    """Split a SQL script into individual statements, ignoring empty ones."""
    return [s.strip() for s in sql.split(";") if s.strip()]
