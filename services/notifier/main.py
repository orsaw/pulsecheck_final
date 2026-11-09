import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pika

QUEUE = "status.changed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("notifier")


def format_alert(event: dict) -> str:
    target = event.get("target_id", "?")
    old = event.get("old_status", "?")
    new = event.get("new_status", "?")
    when = event.get("changed_at", "?")
    if new == "DOWN":
        return f"[ALERT] Target {target} is DOWN (was {old}) at {when}"
    if new == "UP":
        return f"[ALERT] Target {target} is UP again (was {old}) at {when}"
    return f"[ALERT] Target {target} changed {old} -> {new} at {when}"


def on_message(channel, method, properties, body):
    try:
        event = json.loads(body)
        log.info(format_alert(event))
    except (ValueError, AttributeError):
        log.error("Bad message, skipped: %r", body)
    channel.basic_ack(delivery_tag=method.delivery_tag)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"status":"ok","service":"notifier"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", 8000), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def main():
    url = os.environ["RABBITMQ_URL"]
    start_health_server()
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(url))
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
            log.info("Notifier started, waiting for '%s'", QUEUE)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ is not available, retry in 3 s")
            time.sleep(3)


if __name__ == "__main__":
    main()
