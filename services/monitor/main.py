"""Monitor service.

Stores monitoring targets in SQLite, gives a REST API to manage them
and, in a background thread, publishes 'check.task' messages to RabbitMQ
when a target is due for a check.
"""
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone

import pika
from fastapi import APIRouter, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field, field_validator

TASK_QUEUE = "check.task"
DB_PATH = os.getenv("DB_PATH", "/data/monitor.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("monitor")


# ---------------------------------------------------------------- database

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with closing(db()) as conn, conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS targets (
                   id           INTEGER PRIMARY KEY AUTOINCREMENT,
                   owner_id     TEXT    NOT NULL,
                   name         TEXT    NOT NULL,
                   url          TEXT    NOT NULL,
                   interval_sec INTEGER NOT NULL,
                   created_at   TEXT    NOT NULL
               )"""
        )


def all_targets():
    with closing(db()) as conn:
        rows = conn.execute("SELECT id, url, interval_sec FROM targets").fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------- scheduler

def run_once(publish, due, now=None):
    """One scheduler tick: publish tasks for targets that are due.

    publish(task_dict) sends one task. due maps target id -> time of the
    next check. A target that is not in `due` yet is checked at once.
    """
    now = time.monotonic() if now is None else now
    targets = all_targets()
    ids = {t["id"] for t in targets}
    for stale in [i for i in due if i not in ids]:
        del due[stale]  # target was deleted
    for t in targets:
        if due.get(t["id"], 0) <= now:
            publish({
                "target_id": str(t["id"]),
                "url": t["url"],
                "scheduled_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
            # Set the next time only after a successful publish.
            due[t["id"]] = now + t["interval_sec"]


def scheduler_loop(rabbitmq_url: str, stop: threading.Event):
    due: dict = {}
    while not stop.is_set():
        connection = None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            channel = connection.channel()
            channel.queue_declare(queue=TASK_QUEUE, durable=True)
            log.info("Scheduler connected to RabbitMQ")

            def publish(task):
                channel.basic_publish(
                    exchange="",
                    routing_key=TASK_QUEUE,
                    body=json.dumps(task),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                log.info("check.task sent: target=%s url=%s",
                         task["target_id"], task["url"])

            while not stop.is_set():
                run_once(publish, due)
                connection.sleep(1)  # waits 1 s and keeps the connection alive
        except Exception as exc:  # keep the thread alive, whatever happens
            log.warning("Scheduler error: %s. Retry in 3 s", exc)
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    pass


# --------------------------------------------------------------------- API

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    stop = threading.Event()
    thread = threading.Thread(
        target=scheduler_loop,
        args=(os.environ["RABBITMQ_URL"], stop),
        daemon=True,
    )
    thread.start()
    yield
    stop.set()


app = FastAPI(title="PulseCheck Monitor", lifespan=lifespan)
router = APIRouter(prefix="/api/monitor")


class TargetIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    url: str = Field(max_length=2000)
    interval_sec: int = Field(ge=10, le=3600)

    @field_validator("url")
    @classmethod
    def url_must_be_http(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return value


def current_user(x_user_id: str | None) -> str:
    """User id comes from the header set by the gateway."""
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(status_code=401, detail="X-User-Id header is missing")
    return x_user_id.strip()


@router.post("/targets", status_code=201)
def create_target(body: TargetIn, x_user_id: str | None = Header(default=None)):
    owner = current_user(x_user_id)
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO targets (owner_id, name, url, interval_sec, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (owner, body.name, body.url, body.interval_sec, created_at),
        )
        target_id = cur.lastrowid
    return {
        "id": target_id,
        "owner_id": owner,
        "name": body.name,
        "url": body.url,
        "interval_sec": body.interval_sec,
        "created_at": created_at,
    }


@router.get("/targets")
def list_targets(x_user_id: str | None = Header(default=None)):
    owner = current_user(x_user_id)
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, owner_id, name, url, interval_sec, created_at "
            "FROM targets WHERE owner_id = ? ORDER BY id",
            (owner,),
        ).fetchall()
    return [dict(r) for r in rows]


@router.delete("/targets/{target_id}", status_code=204)
def delete_target(target_id: int, x_user_id: str | None = Header(default=None)):
    owner = current_user(x_user_id)
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "DELETE FROM targets WHERE id = ? AND owner_id = ?",
            (target_id, owner),
        )
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Target not found")
    return Response(status_code=204)


app.include_router(router)


@app.get("/health")
def health():
    return {"status": "ok", "service": "monitor"}
