4
# check.result (expected from Checker Worker)
{
  "target_id": "uuid",
  "timestamp": "ISO8601",
  "status": "UP|DOWN",
  "response_time_ms": 123,
  "check_type": "http|tcp|ping",
  "error": null
}

# status.changed (published by Status Service)
{
  "target_id": "uuid",
  "old_status": "UP",
  "new_status": "DOWN",
  "changed_at": "ISO8601"
}
