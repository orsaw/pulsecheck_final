# main.py — Status Service (author: Katya; adapted for Docker by Ulyana)
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

import pika
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# --- 0. SETTINGS (from environment variables) ---
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
DB_PATH = os.getenv("DB_PATH", "./status.db")

# --- 1. DATABASE ---
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class CheckResult(Base):
    """History: one row per check."""
    __tablename__ = "check_results"
    id = Column(Integer, primary_key=True, index=True)
    target_id = Column(String, index=True)
    status = Column(String)
    response_time_ms = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)


class TargetStatus(Base):
    """Current state: one row per target."""
    __tablename__ = "target_statuses"
    target_id = Column(String, primary_key=True, index=True)
    current_status = Column(String)
    last_check = Column(DateTime, default=datetime.utcnow)
    uptime_percentage = Column(Float, default=100.0)


Base.metadata.create_all(bind=engine)

# --- 2. METRICS ---
CHECKS_RECEIVED = Counter("checks_received_total", "Total results consumed from queue")
STATUS_CHANGES = Counter("status_changes_total", "UP<->DOWN transitions")


# --- 3. RABBITMQ ---
def rabbit_connection():
    """Connect to RabbitMQ. If it is not ready yet, wait and try again."""
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ is not ready, retry in 3 s")
            time.sleep(3)


def callback(ch, method, properties, body):
    """Called for every check.result message."""
    db = SessionLocal()
    try:
        data = json.loads(body)
        target_id = str(data.get("target_id"))
        new_status = data.get("status")
        CHECKS_RECEIVED.inc()

        # 1. Save history
        db.add(CheckResult(
            target_id=target_id,
            status=new_status,
            response_time_ms=data.get("response_time_ms") or 0,
            timestamp=datetime.utcnow(),
        ))

        # 2. Update current state and detect a status change
        target = db.query(TargetStatus).filter(TargetStatus.target_id == target_id).first()
        if target is None:
            db.add(TargetStatus(target_id=target_id, current_status=new_status))
        else:
            if target.current_status != new_status:
                print(f"🚨 ALERT: Target {target_id} changed {target.current_status} -> {new_status}")
                STATUS_CHANGES.inc()
                event = {
                    "target_id": target_id,
                    "old_status": target.current_status,
                    "new_status": new_status,
                    "changed_at": datetime.utcnow().isoformat() + "Z",
                }
                ch.queue_declare(queue="status.changed", durable=True)
                ch.basic_publish(exchange="", routing_key="status.changed", body=json.dumps(event))
                print(" 📤 status.changed sent")
                target.current_status = new_status
            target.last_check = datetime.utcnow()

        db.commit()
    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        db.close()


def process_queue():
    """Listen to check.result forever. Reconnect if the connection is lost."""
    while True:
        try:
            connection = rabbit_connection()
            channel = connection.channel()
            channel.queue_declare(queue="check.result", durable=True)
            channel.basic_consume(queue="check.result", on_message_callback=callback, auto_ack=True)
            print(" [*] Status Service is waiting for messages")
            channel.start_consuming()
        except pika.exceptions.AMQPError as e:
            print(f"RabbitMQ connection lost: {e}. Reconnecting in 3 s")
            time.sleep(3)


# --- 4. API ---
@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=process_queue, daemon=True).start()
    yield


app = FastAPI(title="Status Service API", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "status"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/status/")
def list_statuses():
    db = SessionLocal()
    targets = db.query(TargetStatus).all()
    db.close()
    return [
        {"target_id": t.target_id, "status": t.current_status, "last_check": t.last_check}
        for t in targets
    ]


@app.get("/api/status/{target_id}")
def get_status(target_id: str):
    db = SessionLocal()
    target = db.query(TargetStatus).filter(TargetStatus.target_id == target_id).first()
    db.close()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")
    return {"target_id": target.target_id, "status": target.current_status, "last_check": target.last_check}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)
