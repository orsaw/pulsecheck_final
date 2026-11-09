import pika, json, random, time
from datetime import datetime

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='check.result')

print(" [*] Фейковый паблишер запущен. Отправляем данные раз в 5 секунд...")

while True:
    status = "DOWN" if random.random() < 0.2 else "UP"

    msg = {
        "target_id": "demo-target-1",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": status,
        "response_time_ms": random.randint(50, 500) if status == "UP" else 0,
        "check_type": "http",
        "error": None if status == "UP" else "Timeout error"
    }

    channel.basic_publish(exchange='', routing_key='check.result', body=json.dumps(msg))
    print(f" [x] Отправлено: {msg['target_id']} -> {msg['status']}")
    time.sleep(5)
