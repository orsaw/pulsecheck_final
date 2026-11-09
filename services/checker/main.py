"""Checker service.

Takes a task from the 'check.task' queue, makes an HTTP GET request
to the target URL and publishes the result to 'check.result'.
Many copies can run at the same time: they all read one queue
and RabbitMQ gives each task to only one of them.
"""
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pika
import requests

TASK_QUEUE = "check.task"
RESULT_QUEUE = "check.result"
TIMEOUT_SEC = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("checker")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def error_text(exc: Exception) -> str:
    """Short human-readable error for the dashboard."""
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    text = str(exc).lower()
    if "refused" in text:
        return "connection refused"
    if "name or service not known" in text or "nodename nor servname" in text \
            or "temporary failure in name resolution" in text:
        return "DNS error"
    if isinstance(exc, requests.exceptions.SSLError):
        return "SSL error"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection error"
    return "request failed"


def check_url(url: str) -> dict:
    """Make one HTTP check. Returns the fields of check.result."""
    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        return {"status": "DOWN", "response_time_ms": 0, "error": "invalid url"}
    started = time.monotonic()
    try:
        response = requests.get(
            url,
            timeout=TIMEOUT_SEC,
            headers={"User-Agent": "PulseCheck/1.0"},
        )
    except requests.exceptions.RequestException as exc:
        return {"status": "DOWN", "response_time_ms": 0, "error": error_text(exc)}
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if response.status_code < 400:
        return {"status": "UP", "response_time_ms": elapsed_ms, "error": None}
    return {
        "status": "DOWN",
        "response_time_ms": 0,
        "error": f"HTTP {response.status_code}",
    }


def on_task(channel, method, properties, body):
    try:
        task = json.loads(body)
        target_id = str(task["target_id"])
        url = task["url"]
    except (ValueError, KeyError, TypeError):
        log.error("Bad task, skipped: %r", body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    result = check_url(url)
    message = {
        "target_id": target_id,
        "timestamp": now_iso(),
        "status": result["status"],
        "response_time_ms": result["response_time_ms"],
        "check_type": "http",
        "error": result["error"],
    }
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    # Ack only after the result is published: if we crash before,
    # RabbitMQ gives the task to another checker.
    channel.basic_ack(delivery_tag=method.delivery_tag)
    log.info("target=%s url=%s -> %s (%s ms) %s", target_id, url,
             message["status"], message["response_time_ms"],
             message["error"] or "")


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"status":"ok","service":"checker"}'
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
            channel.queue_declare(queue=TASK_QUEUE, durable=True)
            channel.queue_declare(queue=RESULT_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_task)
            log.info("Checker started, waiting for '%s'", TASK_QUEUE)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ is not available, retry in 3 s")
            time.sleep(3)


if __name__ == "__main__":
    main()
