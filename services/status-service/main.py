# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter
from fastapi.responses import Response
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import pika, json, threading, time
from datetime import datetime
import uvicorn

# --- 1. НАСТРОЙКА БАЗЫ ДАННЫХ ---
# Пока используем SQLite для простоты локальной разработки
SQLALCHEMY_DATABASE_URL = "sqlite:///./status.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Модель таблицы истории
class CheckResult(Base):
    __tablename__ = "check_results"
    id = Column(Integer, primary_key=True, index=True)
    target_id = Column(String, index=True)
    status = Column(String)
    response_time_ms = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Модель текущего состояния (для быстрого доступа)
class TargetStatus(Base):
    __tablename__ = "target_statuses"
    target_id = Column(String, primary_key=True, index=True)
    current_status = Column(String)
    last_check = Column(DateTime, default=datetime.utcnow)
    uptime_percentage = Column(Float, default=100.0)

Base.metadata.create_all(bind=engine)

# --- 2. НАСТРОЙКА МЕТРИК ---
CHECKS_RECEIVED = Counter('checks_received_total', 'Total results consumed from queue')
STATUS_CHANGES = Counter('status_changes_total', 'UP<->DOWN transitions')

# --- 3. СЛУШАТЕЛЬ ОЧЕРЕДИ (RABBITMQ) ---
def process_queue():
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            target_id = data.get("target_id")
            new_status = data.get("status")
            
            CHECKS_RECEIVED.inc()
            db = SessionLocal()
            
            # 1. Сохраняем историю
            history = CheckResult(
                target_id=target_id, 
                status=new_status, 
                response_time_ms=data.get("response_time_ms", 0),
                timestamp=datetime.utcnow()
            )
            db.add(history)
            
            # 2. Проверяем смену статуса
            target = db.query(TargetStatus).filter(TargetStatus.target_id == target_id).first()
            if not target:
                # Первая проверка
                target = TargetStatus(target_id=target_id, current_status=new_status)
                db.add(target)
            else:
                if target.current_status != new_status:
                    # ОПА! СТАТУС СМЕНИЛСЯ!
                    print(f"🚨 ALERT: Target {target_id} changed {target.current_status} -> {new_status}")
                    STATUS_CHANGES.inc()
                    
                    # --- ОТПРАВЛЯЕМ СОБЫТИЕ ДЛЯ ЛЁШИ (Notifier) ---
                    event = {
                        "target_id": target_id,
                        "old_status": target.current_status,
                        "new_status": new_status,
                        "changed_at": datetime.utcnow().isoformat() + "Z"
                    }
                    try:
                        pub_conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                        pub_ch = pub_conn.channel()
                        pub_ch.queue_declare(queue='status.changed')
                        pub_ch.basic_publish(exchange='', routing_key='status.changed', body=json.dumps(event))
                        pub_conn.close()
                        print(f" 📤 Событие status.changed отправлено в RabbitMQ!")
                    except Exception as e:
                        print(f" Ошибка отправки события: {e}")
                    # -----------------------------------------------
                    
                    target.current_status = new_status
                    target.last_check = datetime.utcnow()
            
            db.commit()
            db.close()
        except Exception as e:
            print(f"Error processing message: {e}")

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='check.result')
    channel.basic_consume(queue='check.result', on_message_callback=callback, auto_ack=True)
    print(' [*] Status Service ждет сообщений из RabbitMQ. Для выхода нажмите CTRL+C')
    channel.start_consuming()

# --- 4. API ДЛЯ ДАШБОРДА (FastAPI) ---
app = FastAPI(title="Status Service API")

# Разрешаем CORS, чтобы наш HTML дашборд мог стучаться к API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/status/{target_id}")
def get_status(target_id: str):
    db = SessionLocal()
    target = db.query(TargetStatus).filter(TargetStatus.target_id == target_id).first()
    db.close()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")
    return {
        "target_id": target.target_id,
        "status": target.current_status,
        "last_check": target.last_check
    }

# Запускаем слушатель очереди в отдельном потоке при старте сервиса
@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=process_queue, daemon=True)
    thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)
