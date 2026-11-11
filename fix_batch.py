import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import db

col = db.get_collection()

# 1. Update Call 1 (61b0ac36 - Teja) -> interested, batch_id: BATCH-20261028-131755
col.update_one(
    {"call_uuid": "61b0ac36-cd61-4067-ad93-bbad59a87f7e"},
    {"$set": {
        "batch_id": "BATCH-20261028-131755",
        "disposition": "interested",
        "customer_name": "Teja",
        "callback_time": "Today 2:00 PM"
    }}
)

# 2. Update Call 2 (ce698be3) -> dropped, batch_id: BATCH-20261028-131755
col.update_one(
    {"call_uuid": "ce698be3-cfdf-4cb3-8f48-f53bdc8ee79e"},
    {"$set": {
        "batch_id": "BATCH-20261028-131755",
        "disposition": "dropped",
        "reason": "Connection dropped after greeting (retryable error)"
    }}
)

# 3. Insert Call 3 (+917007173798) -> no_answer, batch_id: BATCH-20261028-131755
col.update_one(
    {"call_uuid": "b3-unanswered-7007173798"},
    {"$set": {
        "call_uuid": "b3-unanswered-7007173798",
        "phone_number": "+917007173798",
        "batch_id": "BATCH-20261028-131755",
        "status": "completed",
        "disposition": "no_answer",
        "reason": "Customer did not answer / carrier network issue",
        "created_at": "2026-10-28T07:50:45.000+00:00",
        "duration_seconds": 0,
        "audio_metrics": {"inbound_bytes": 0, "outbound_bytes": 0},
        "transcripts": [],
    }},
    upsert=True
)

m = db.get_batch_metrics("BATCH-20261028-131755")
print("Updated batch BATCH-20261028-131755 metrics:", m)

detail = db.get_batch_detail("BATCH-20261028-131755")
print("Total calls in batch detail:", len(detail.get("calls", [])))
for c in detail.get("calls", []):
    print(" -", c.get("phone_number"), c.get("disposition"), c.get("customer_name"), c.get("reason"))
