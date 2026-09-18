import json, os
from datetime import datetime, timezone

_JSON_FIELDS = ("payload", "metrics")
_REDACT_KEYS = ("api_key", "authorization", "token", "openai_api_key", "telegram_bot_token")

def envelope_to_fields(env: dict) -> dict:
    out = {}
    for k, v in env.items():
        out[k] = json.dumps(v) if k in _JSON_FIELDS else str(v)
    return out

def fields_to_envelope(fields: dict) -> dict:
    out = {}
    for k, v in fields.items():
        out[k] = json.loads(v) if k in _JSON_FIELDS else v
    return out

def _redact(env: dict) -> dict:
    clean = dict(env)
    for k in list(clean):
        if k.lower() in _REDACT_KEYS:
            clean[k] = "***"
    return clean

class A2ABus:
    def __init__(self, client, requests_stream="a2a:requests",
                 responses_stream="a2a:responses", heartbeat_stream="a2a:heartbeats",
                 audit_path="a2a_audit.jsonl"):
        self.client = client
        self.REQUESTS = requests_stream
        self.RESPONSES = responses_stream
        self.HEARTBEATS = heartbeat_stream
        self.audit_path = audit_path

    def publish(self, stream: str, envelope: dict) -> str:
        entry_id = self.client.xadd(stream, envelope_to_fields(envelope))
        self._audit({"stream": stream, "id": entry_id,
                     "at": datetime.now(timezone.utc).isoformat(),
                     **_redact(envelope)})
        return entry_id

    def read(self, stream: str, last_id: str = "0", block_ms: int = 1000, count: int = 10):
        resp = self.client.xread({stream: last_id}, count=count, block=block_ms)
        items = []
        for _stream, entries in (resp or []):
            for entry_id, fields in entries:
                items.append((entry_id, fields_to_envelope(fields)))
        return items

    def _audit(self, record: dict) -> None:
        with open(self.audit_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
