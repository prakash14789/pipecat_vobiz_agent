import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env", override=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "rag-bot_test")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "vobiz_ai_calls")
MONGO_BATCHES_COLLECTION_NAME = os.getenv("MONGO_BATCHES_COLLECTION_NAME", "campaign_batches")

_client: Optional[MongoClient] = None
_db: Optional[Database] = None
_collection: Optional[Collection] = None
_batches_col: Optional[Collection] = None

# Initialize MongoDB client and ensure collection indexes
def get_collection() -> Optional[Collection]:
    global _client, _db, _collection
    if _collection is not None:
        return _collection
    try:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        _db = _client[MONGO_DB_NAME]
        _collection = _db[MONGO_COLLECTION_NAME]
        _collection.create_index([("call_uuid", ASCENDING)], unique=True)
        _collection.create_index([("phone_number", ASCENDING)])
        _collection.create_index([("batch_id", ASCENDING)])
        _collection.create_index([("created_at", DESCENDING)])
        return _collection
    except Exception as exc:
        logger.error(f"MongoDB connection failed: {exc}")
        return None

# Initialize campaign batches collection
def get_batches_collection() -> Optional[Collection]:
    global _client, _db, _batches_col
    if _batches_col is not None:
        return _batches_col
    col = get_collection()
    if _db is not None:
        _batches_col = _db[MONGO_BATCHES_COLLECTION_NAME]
        _batches_col.create_index([("batch_id", ASCENDING)], unique=True)
        _batches_col.create_index([("created_at", DESCENDING)])
        return _batches_col
    return None

# Current UTC timestamp in ISO 8601 format
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

# Create or upsert a call record
def record_call_start(
    call_uuid: str,
    phone_number: str = "",
    batch_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    col = get_collection()
    initial_doc = {
        "call_uuid": call_uuid,
        "phone_number": phone_number,
        "status": "in_progress",
        "created_at": now_iso(),
        "start_time": time.time(),
        "disposition": "not_talking",
        "customer_name": None,
        "callback_time": None,
        "reason": None,
        "transcripts": [],
        "metadata": metadata or {},
    }
    if col is not None:
        try:
            update_op: Dict[str, Any] = {"$setOnInsert": initial_doc}
            if batch_id:
                update_op["$set"] = {"batch_id": batch_id}
            else:
                initial_doc["batch_id"] = None
            col.update_one({"call_uuid": call_uuid}, update_op, upsert=True)
        except Exception as exc:
            logger.error(f"Failed to record call start for {call_uuid}: {exc}")
    return initial_doc

# Update call record fields
def update_call(call_uuid: str, updates: Dict[str, Any]) -> None:
    col = get_collection()
    if col is not None:
        try:
            updates["updated_at"] = now_iso()
            col.update_one({"call_uuid": call_uuid}, {"$set": updates}, upsert=True)
        except Exception as exc:
            logger.error(f"Failed to update call {call_uuid}: {exc}")

# Append a spoken turn to the call transcript
def append_transcript(call_uuid: str, role: str, text: str) -> None:
    col = get_collection()
    if not text or not text.strip():
        return
    turn = {
        "role": role,
        "text": text.strip(),
        "timestamp": now_iso(),
    }
    if col is not None:
        try:
            col.update_one({"call_uuid": call_uuid}, {"$push": {"transcripts": turn}, "$set": {"updated_at": now_iso()}})
        except Exception as exc:
            logger.error(f"Failed to append transcript for {call_uuid}: {exc}")

# Record final disposition and parameters from tool call or fallback
def record_disposition(
    call_uuid: str,
    disposition: str,
    customer_name: Optional[str] = None,
    callback_time: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    updates: Dict[str, Any] = {"disposition": disposition}
    if customer_name:
        updates["customer_name"] = customer_name
    if callback_time:
        updates["callback_time"] = callback_time
    if reason:
        updates["reason"] = reason
    update_call(call_uuid, updates)

import diagnostics

# Evaluate complete call evidence at call conclusion to determine final truthful disposition
def resolve_end_of_call_disposition(
    doc: Optional[Dict[str, Any]],
    tracer: Any,
    summary: Dict[str, Any],
    fallback_disposition: Optional[str] = None,
) -> Dict[str, Any]:
    audio_out = summary.get("diagnostic", {}).get("audio_metrics", {}).get("outbound_bytes", 0)
    audio_in = summary.get("diagnostic", {}).get("audio_metrics", {}).get("inbound_bytes", 0)
    hangup_cause = str(summary.get("diagnostic", {}).get("hangup", {}).get("cause") or "").upper()
    duration = doc.get("duration_seconds", 0) if doc else 0
    transcripts = doc.get("transcripts", []) if doc else []
    caller_turns = [t for t in transcripts if t.get("role") == "caller"]

    # 1. Unanswered, busy, or rejected calls
    if hangup_cause == "USER_BUSY":
        return {"disposition": "busy", "reason": "Customer line busy"}
    if hangup_cause in ("NO_ANSWER", "TIMEOUT", "CALL_REJECTED", "ORIGINATOR_CANCEL", "REJECTED") or (audio_out == 0 and audio_in == 0 and len(transcripts) == 0):
        return {"disposition": "no_answer", "reason": "Customer did not answer / carrier network issue"}

    # 2. No caller audio received (agent spoke greeting, then line dropped or remained completely silent)
    if len(caller_turns) == 0:
        if audio_out > 0 and (duration is None or duration < 35):
            return {"disposition": "dropped", "reason": "Connection dropped after greeting before customer could speak (retryable)"}
        return {"disposition": "not_talking", "reason": "Customer remained silent throughout call"}

    # 3. Caller spoke - evaluate conversational evidence
    full_caller = " ".join(t.get("text", "") for t in caller_turns).lower()

    if any(k in full_caller for k in ["wrong number", "galat number", "wrong call", "tappu number"]):
        return {"disposition": "wrong_number", "reason": "Caller indicated wrong number"}

    if tracer.disposition == "not_interested" or any(k in full_caller for k in ["not interested", "nahi chahiye", "vaddu", "no interest", "don't want", "detail nahi", "nahi share"]):
        return {"disposition": "not_interested", "reason": tracer.reason or "Caller declined / not interested"}

    # Check for interest confirmation
    if tracer.disposition == "interested" or any(k in full_caller for k in ["interested", "haan", "yes", "ha"]):
        name = tracer.customer_name or (doc.get("customer_name") if doc else None)
        cb = tracer.callback_time or (doc.get("callback_time") if doc else None)
        agent_turns = [t for t in transcripts if t.get("role") == "agent"]
        full_agent = " ".join(t.get("text", "") for t in agent_turns).lower()
        farewell_delivered = any(w in full_agent for w in ["goodbye", "have a nice day", "shubh ho", "dhanyavad", "bye"])

        if name or cb or tracer.disposition == "interested" or farewell_delivered:
            return {
                "disposition": "interested",
                "customer_name": name or (doc.get("metadata", {}).get("customer_name") if doc else None),
                "callback_time": cb,
                "reason": tracer.reason or "Caller expressed interest and concluded conversation",
            }
        else:
            return {
                "disposition": "dropped",
                "reason": "Caller expressed interest but call dropped before collecting details (high-priority retry)",
            }

    if tracer.disposition:
        return {"disposition": tracer.disposition, "reason": tracer.reason}
    if fallback_disposition:
        return {"disposition": fallback_disposition, "reason": None}

    return {"disposition": "completed", "reason": "Conversation finished"}

# Finalize call duration, disposition, and diagnostic health
def finalize_call(
    call_uuid: str,
    status: str = "completed",
    batch_id: Optional[str] = None,
    fallback_disposition: Optional[str] = None,
    disposition: Optional[str] = None,
    reason: Optional[str] = None,
    **kwargs: Any,
) -> None:
    fallback_disp = disposition or fallback_disposition
    col = get_collection()
    tracer = diagnostics.get_tracer(call_uuid)
    summary = tracer.finalize()
    b_id = batch_id or tracer.batch_id

    if col is not None:
        try:
            doc = col.find_one({"call_uuid": call_uuid})
            if not b_id and doc:
                b_id = doc.get("batch_id")

            duration = None
            if doc and doc.get("start_time"):
                duration = round(time.time() - doc["start_time"], 2)

            resolved = resolve_end_of_call_disposition(doc, tracer, summary, fallback_disp)

            update_fields: Dict[str, Any] = {
                "status": status,
                "disposition": resolved.get("disposition", "not_talking"),
                "ended_at": now_iso(),
                "ended_at_epoch": time.time(),
                "duration_seconds": duration,
                "events": summary.get("events", []),
                "diagnostic": summary.get("diagnostic", {}),
                "audio_metrics": summary.get("diagnostic", {}).get("audio_metrics", {}),
            }
            if resolved.get("customer_name"):
                update_fields["customer_name"] = resolved["customer_name"]
            if resolved.get("callback_time"):
                update_fields["callback_time"] = resolved["callback_time"]
            final_reason = reason or resolved.get("reason")
            if final_reason:
                update_fields["reason"] = final_reason
            if b_id:
                update_fields["batch_id"] = b_id

            col.update_one({"call_uuid": call_uuid}, {"$set": update_fields}, upsert=True)
        except Exception as exc:
            logger.error(f"Failed to finalize call {call_uuid}: {exc}")

# Retrieve a single call record by uuid
def get_call(call_uuid: str) -> Optional[Dict[str, Any]]:
    col = get_collection()
    if col is not None:
        try:
            doc = col.find_one({"call_uuid": call_uuid}, {"_id": 0})
            if doc:
                doc["turns"] = doc.get("transcripts", [])
                doc["duration"] = doc.get("duration_seconds", 0) or 0
                doc["started"] = doc.get("created_at", "")
                doc["ended"] = doc.get("ended_at", "")
                doc["diagnostic"] = doc.get("diagnostic") or {}
                doc["events"] = doc.get("events") or []
                doc["audio_metrics"] = doc.get("audio_metrics") or {}
                return doc
        except Exception as exc:
            logger.error(f"Failed to query call {call_uuid}: {exc}")
    return None

# Retrieve recent call records
def get_recent_calls(limit: int = 50) -> List[Dict[str, Any]]:
    col = get_collection()
    if col is not None:
        try:
            cursor = col.find({}, {"_id": 0}).sort("created_at", DESCENDING).limit(limit)
            calls = list(cursor)
            for c in calls:
                c["turns"] = c.get("transcripts", [])
                c["duration"] = c.get("duration_seconds", 0) or 0
                c["started"] = c.get("created_at", "")
                c["ended"] = c.get("ended_at", "")
                c["diagnostic"] = c.get("diagnostic") or {}
                c["events"] = c.get("events") or []
                c["audio_metrics"] = c.get("audio_metrics") or {}
            return calls
        except Exception as exc:
            logger.error(f"Failed to query recent calls: {exc}")
    return []

# Save or update a campaign batch document
def save_batch(batch_doc: Dict[str, Any]) -> None:
    col = get_batches_collection()
    if col is not None:
        try:
            batch_doc["updated_at"] = now_iso()
            col.update_one({"batch_id": batch_doc["batch_id"]}, {"$set": batch_doc}, upsert=True)
        except Exception as exc:
            logger.error(f"Failed to save batch {batch_doc.get('batch_id')}: {exc}")

# Finalize a campaign batch
def finalize_batch(batch_id: str, status: str = "completed") -> None:
    col = get_batches_collection()
    if col is not None:
        try:
            col.update_one(
                {"batch_id": batch_id},
                {"$set": {"status": status, "ended_at": now_iso(), "updated_at": now_iso()}},
            )
        except Exception as exc:
            logger.error(f"Failed to finalize batch {batch_id}: {exc}")

# Calculate batch-wise metrics for a given batch_id
def get_batch_metrics(batch_id: str) -> Dict[str, int]:
    col = get_collection()
    metrics = {
        "dialed": 0,
        "interested": 0,
        "not_interested": 0,
        "wrong_number": 0,
        "not_talking": 0,
        "no_answer": 0,
        "dropped": 0,
    }
    if col is not None and batch_id:
        try:
            calls = list(col.find({"batch_id": batch_id}, {"disposition": 1, "_id": 0}))
            metrics["dialed"] = len(calls)
            for c in calls:
                disp = c.get("disposition")
                if disp == "interested":
                    metrics["interested"] += 1
                elif disp in ("not_interested", "declined"):
                    metrics["not_interested"] += 1
                elif disp == "wrong_number":
                    metrics["wrong_number"] += 1
                elif disp in ("dropped", "system_error"):
                    metrics["dropped"] += 1
                elif disp == "not_talking":
                    metrics["not_talking"] += 1
                elif disp in ("no_answer", "busy", "failed"):
                    metrics["no_answer"] += 1
        except Exception as exc:
            logger.error(f"Failed to calculate metrics for batch {batch_id}: {exc}")
    return metrics

# Retrieve all campaign batches with live metrics
def get_batches() -> List[Dict[str, Any]]:
    col = get_batches_collection()
    batches: List[Dict[str, Any]] = []
    if col is not None:
        try:
            cursor = col.find({}, {"_id": 0}).sort("created_at", DESCENDING)
            for b in cursor:
                bid = b.get("batch_id", "")
                m = get_batch_metrics(bid)
                total = b.get("total_leads", 0)
                m["pending"] = max(0, total - m["dialed"])
                b["metrics"] = m
                batches.append(b)
        except Exception as exc:
            logger.error(f"Failed to query batches: {exc}")
    return batches

# Retrieve a batch document and all its associated calls
def get_batch_detail(batch_id: str) -> Dict[str, Any]:
    b_col = get_batches_collection()
    c_col = get_collection()
    batch_doc = None
    if b_col is not None:
        batch_doc = b_col.find_one({"batch_id": batch_id}, {"_id": 0})
    if batch_doc:
        m = get_batch_metrics(batch_id)
        m["pending"] = max(0, batch_doc.get("total_leads", 0) - m["dialed"])
        batch_doc["metrics"] = m

    calls: List[Dict[str, Any]] = []
    if c_col is not None:
        try:
            cursor = c_col.find({"batch_id": batch_id}, {"_id": 0}).sort("created_at", DESCENDING)
            for c in cursor:
                c["turns"] = c.get("transcripts", [])
                c["duration"] = c.get("duration_seconds", 0) or 0
                c["started"] = c.get("created_at", "")
                c["ended"] = c.get("ended_at", "")
                calls.append(c)
        except Exception as exc:
            logger.error(f"Failed to query calls for batch {batch_id}: {exc}")

    return {"batch": batch_doc or {"batch_id": batch_id, "status": "unknown"}, "calls": calls}

# Delete a batch record
def delete_batch(batch_id: str) -> bool:
    col = get_batches_collection()
    if col is not None:
        try:
            col.delete_one({"batch_id": batch_id})
            return True
        except Exception as exc:
            logger.error(f"Failed to delete batch {batch_id}: {exc}")
    return False
