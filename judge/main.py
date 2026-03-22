from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import os
from judge_core import run_mysql_judge, run_postgres_judge
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="SQL Judge API")

API_SECRET = os.getenv("API_SECRET", "changeme")


class JudgeRequest(BaseModel):
    db_type: str          # "mysql" or "postgres"
    setup_sql: str        # Creates tables, inserts data
    user_sql: str         # Student's submitted query
    expected_sql: str     # Reference correct query


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/judge")
def judge(
    req: JudgeRequest,
    x_api_secret: str = Header(None)
):
    if x_api_secret != API_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if req.db_type == "mysql":
        result = run_mysql_judge(req.setup_sql, req.user_sql, req.expected_sql)
    elif req.db_type == "postgres":
        result = run_postgres_judge(req.setup_sql, req.user_sql, req.expected_sql)
    else:
        raise HTTPException(status_code=400, detail="db_type must be 'mysql' or 'postgres'")

    return result
