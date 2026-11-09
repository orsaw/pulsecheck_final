import datetime
import os
import sqlite3

import bcrypt
import jwt
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

DB_PATH = os.getenv("DB_PATH", "/data/auth.db")
JWT_SECRET = os.environ["JWT_SECRET"]
JWT_EXPIRES_MIN = int(os.getenv("JWT_EXPIRES_MIN", "60"))

app = FastAPI(title="PulseCheck Auth")


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
with db() as conn:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " username TEXT UNIQUE NOT NULL,"
        " password_hash TEXT NOT NULL)"
    )


class Credentials(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=8, max_length=72)


def decode_token(authorization: str | None) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.removeprefix("Bearer ")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@app.get("/health")
def health():
    return {"status": "ok", "service": "auth"}


@app.post("/register", status_code=201)
def register(c: Credentials):
    password_hash = bcrypt.hashpw(c.password.encode(), bcrypt.gensalt()).decode()
    try:
        with db() as conn:
            cur = conn.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (c.username, password_hash),
            )
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Username already taken")
    return {"id": cur.lastrowid, "username": c.username}


@app.post("/login")
def login(c: Credentials):
    with db() as conn:
        row = conn.execute(
            "SELECT id, password_hash FROM users WHERE username = ?", (c.username,)
        ).fetchone()
    if row is None or not bcrypt.checkpw(c.password.encode(), row["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Wrong username or password")

    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=JWT_EXPIRES_MIN)
    token = jwt.encode(
        {"sub": str(row["id"]), "username": c.username, "exp": expires},
        JWT_SECRET,
        algorithm="HS256",
    )
    return {"access_token": token, "token_type": "bearer", "expires_in": JWT_EXPIRES_MIN * 60}


@app.get("/verify")
def verify(response: Response, authorization: str | None = Header(default=None)):
    claims = decode_token(authorization)
    response.headers["X-User-Id"] = claims["sub"]
    response.headers["X-Username"] = claims["username"]
    return {"ok": True}


@app.get("/me")
def me(authorization: str | None = Header(default=None)):
    claims = decode_token(authorization)
    return {"id": int(claims["sub"]), "username": claims["username"]}
