import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from loguru import logger

import db
import diagnostics
import vobiz_api

BASE_DIR = Path(__file__).parent

class OutboundDialer:
    """Manages the outbound call queue, OBD campaign execution, and live call tracking."""

    def __init__(self):
        self.status = "idle"
        self.queue: List[Dict[str, Any]] = []
        self.active_call: Optional[Dict[str, Any]] = None
        self.last_call: Optional[Dict[str, Any]] = None
        self.dialer_task: Optional[asyncio.Task] = None
        self.cooldown_seconds = float(os.getenv("OUTBOUND_COOLDOWN_SEC", "3.0"))
        self.batches: List[Dict[str, Any]] = []
        self.current_batch_id: Optional[str] = None
        self.total_batch_leads: int = 0

    # Fetch pending calls from CRM using the node service
    def fetch_crm_pending(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        script_path = BASE_DIR / "crm_service.cjs"
        cmd = ["node", str(script_path), start_date or "", end_date or ""]
        try:
            res = subprocess.run(cmd, cwd=str(BASE_DIR), capture_output=True, text=True, timeout=25)
            if res.returncode == 0 and res.stdout.strip():
                data = json.loads(res.stdout.strip())
                if isinstance(data, dict):
                    return data.get("calls", [])
                return data
            logger.warning(f"CRM fetch non-zero exit: {res.stderr}")
        except Exception as exc:
            logger.error(f"CRM pending fetch failed: {exc}")
        return []

    # Queue a list of leads for outbound calling
    def queue_leads(
        self,
        leads: List[Dict[str, Any]],
        batch_name: Optional[str] = None,
        source_filter: Optional[str] = None,
    ) -> str:
        batch_id = f"BATCH-{datetime_now_str()}"
        self.current_batch_id = batch_id
        self.queue.clear()

        for lead in leads:
            entry = {
                "id": lead.get("id") or str(time.time()),
                "phone_number": lead.get("phone_number") or lead.get("customerNumber"),
                "customer_name": lead.get("customer_name") or lead.get("customerName", ""),
                "source": lead.get("source") or lead.get("marketing_source", ""),
                "batch_id": batch_id,
                "status": "pending",
                "queued_at": time.time(),
            }
            if entry["phone_number"]:
                self.queue.append(entry)

        batch_doc = {
            "batch_id": batch_id,
            "name": batch_name or f"Batch {time.strftime('%Y-%m-%d %H:%M')}",
            "source_filter": source_filter or "CRM Missed Calls",
            "total_leads": len(self.queue),
            "status": "active",
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "started_at": time.time(),
        }
        db.save_batch(batch_doc)
        self.total_batch_leads = len(self.queue)
        logger.info(f"Queued {len(self.queue)} leads into batch {batch_id}")
        return batch_id

    # Check and clear stuck orphan calls exceeding carrier thresholds
    def _check_watchdog(self):
        if not self.active_call:
            return
        elapsed = time.time() - self.active_call.get("started_at", time.time())
        status = self.active_call.get("status", "dialing")
        uuid = self.active_call.get("call_uuid", "")
        if (status == "dialing" and elapsed > 60.0) or (status == "in_call" and elapsed > 1800.0):
            logger.warning(f"Watchdog terminating stuck call {uuid} ({status}) after {elapsed:.1f}s")
            try:
                db.finalize_call(
                    call_uuid=uuid,
                    status="hung_up",
                    fallback_disposition="no_answer" if status == "dialing" else "dropped",
                )
            except Exception as e:
                logger.error(f"Error in watchdog finalize_call: {e}")
            self.on_call_ended(uuid)

    # Calculate live counters for dialer UI
    def get_kpi_counts(self) -> Dict[str, int]:
        self._check_watchdog()
        is_dialing = 1 if (self.active_call and self.active_call.get("status") == "dialing") else 0
        is_incall = 1 if (self.active_call and self.active_call.get("status") == "in_call") else 0

        # When no batch is active, counters stay clean at 0
        if not self.current_batch_id:
            return {
                "total": len(self.queue),
                "pending": sum(1 for item in self.queue if item["status"] == "pending"),
                "dialing": is_dialing,
                "in_call": is_incall,
                "active": 1 if self.active_call else 0,
                "interested": 0,
                "not_interested": 0,
                "wrong_number": 0,
                "not_talking": 0,
                "no_answer": 0,
                "dropped": 0,
                "retry_scheduled": 0,
            }

        # Calculate batch-wise metrics from MongoDB for current batch
        batch_metrics = db.get_batch_metrics(self.current_batch_id)
        pending_in_queue = sum(1 for item in self.queue if item["status"] == "pending")
        total_leads = len(self.queue) if len(self.queue) > 0 else (getattr(self, "total_batch_leads", 0) or batch_metrics.get("dialed", 0))

        return {
            "total": total_leads,
            "pending": pending_in_queue,
            "dialing": is_dialing,
            "in_call": is_incall,
            "active": 1 if self.active_call else 0,
            "interested": batch_metrics.get("interested", 0),
            "not_interested": batch_metrics.get("not_interested", 0),
            "wrong_number": batch_metrics.get("wrong_number", 0),
            "not_talking": batch_metrics.get("not_talking", 0),
            "no_answer": batch_metrics.get("no_answer", 0),
            "dropped": batch_metrics.get("dropped", 0),
            "retry_scheduled": 0,
        }

    # Set active call reference
    def set_active_call(
        self,
        call_uuid: str,
        phone_number: str,
        customer_name: str = "",
        status: str = "dialing",
        batch_id: Optional[str] = None,
    ):
        self.active_call = {
            "call_uuid": call_uuid,
            "phone_number": phone_number,
            "customer_name": customer_name,
            "batch_id": batch_id or self.current_batch_id,
            "status": status,
            "started_at": time.time(),
        }

    # Update active call phase status
    def set_call_status(self, call_uuid: str, status: str = "in_call"):
        if self.active_call:
            if not call_uuid or self.active_call.get("call_uuid") == call_uuid:
                self.active_call["status"] = status

    # Start automated dialer loop
    def start(self, public_url: str):
        if self.status == "running":
            return
        self.status = "running"
        logger.info("Outbound dialer started")
        if not self.dialer_task or self.dialer_task.done():
            self.dialer_task = asyncio.create_task(self._dial_loop(public_url))

    # Pause dialer loop
    def pause(self):
        self.status = "paused"
        logger.info("Outbound dialer paused")

    # Resume dialer loop
    def resume(self, public_url: str):
        self.start(public_url)

    # Stop dialer and cancel active background loop
    def stop(self):
        self.status = "idle"
        if self.dialer_task and not self.dialer_task.done():
            self.dialer_task.cancel()
        logger.info("Outbound dialer stopped")

    # Clear pending queue and reset batch
    def clear(self):
        self.reset()

    # Reset dialer state, queue, and active batch counters
    def reset(self):
        self.stop()
        self.queue.clear()
        self.active_call = None
        self.last_call = None
        self.total_batch_leads = 0
        if self.current_batch_id:
            db.finalize_batch(self.current_batch_id, status="completed")
            self.current_batch_id = None
        logger.info("Dialer queue and batch counters reset")

    # Continuous background worker placing calls sequentially
    async def _dial_loop(self, public_url: str):
        answer_url = f"{public_url}/answer"
        hangup_url = f"{public_url}/hangup"

        while self.status == "running":
            if self.active_call is not None:
                await asyncio.sleep(1.0)
                continue

            pending_item = next((item for item in self.queue if item["status"] == "pending"), None)
            if not pending_item:
                logger.info("No more pending calls in queue. Switching dialer to idle.")
                self.status = "idle"
                if self.current_batch_id:
                    db.finalize_batch(self.current_batch_id, status="completed")
                break

            target_phone = pending_item["phone_number"]
            pending_item["status"] = "dialing"
            batch_id = pending_item.get("batch_id") or self.current_batch_id
            lead_id = pending_item.get("id", "")
            lead_name = pending_item.get("customer_name", "")
            logger.info(f"Dialer initiating call to {target_phone} (batch: {batch_id})")

            call_answer_url = f"{public_url}/answer?batch_id={batch_id}&lead_id={lead_id}&to={target_phone}"
            call_hangup_url = f"{public_url}/hangup?batch_id={batch_id}&lead_id={lead_id}&to={target_phone}"

            try:
                res = await vobiz_api.place_call(
                    to_number=target_phone,
                    answer_url=call_answer_url,
                    hangup_url=call_hangup_url,
                )
                call_uuid = res.get("request_uuid") or res.get("call_uuid") or pending_item["id"]
                pending_item["call_uuid"] = call_uuid
                self.set_active_call(
                    call_uuid=call_uuid,
                    phone_number=target_phone,
                    customer_name=lead_name,
                    status="dialing",
                    batch_id=batch_id,
                )
                tracer = diagnostics.get_tracer(call_uuid, phone_number=target_phone, batch_id=batch_id)
                tracer.record_event("call_initiated", {
                    "to": target_phone,
                    "customer_name": lead_name,
                    "batch_id": batch_id,
                    "lead_id": lead_id,
                })
                tracer.record_event("carrier_dispatched", {
                    "request_uuid": call_uuid,
                    "message": res.get("message", "call placed"),
                    "api_id": res.get("api_id", ""),
                })
                db.record_call_start(
                    call_uuid=call_uuid,
                    phone_number=target_phone,
                    batch_id=batch_id,
                    metadata={"customer_name": lead_name, "lead_id": lead_id, "status": "dialing"},
                )
            except Exception as exc:
                logger.error(f"Dialer failed to dial {target_phone}: {exc}")
                pending_item["status"] = "failed"
                tracer = diagnostics.get_tracer(pending_item.get("id", str(time.time())), phone_number=target_phone, batch_id=batch_id)
                tracer.record_error("api_call_failed", str(exc))
                self.active_call = None

            # Cooldown interval between calls
            await asyncio.sleep(self.cooldown_seconds)

    # Callback when call terminates to update dialer state
    def on_call_ended(self, call_uuid: str):
        if self.active_call and (not call_uuid or self.active_call.get("call_uuid") == call_uuid):
            duration = round(time.time() - self.active_call.get("started_at", time.time()), 1)
            doc = db.get_call(call_uuid)
            self.last_call = {
                **self.active_call,
                "duration": doc.get("duration", duration) if doc else duration,
                "disposition": doc.get("disposition", "completed") if doc else "completed",
                "ended_at": time.time(),
                "turns": doc.get("turns", []) if doc else [],
                "diagnostic": doc.get("diagnostic", {}) if doc else {},
            }
            self.active_call = None
        for item in self.queue:
            if item.get("call_uuid") == call_uuid:
                item["status"] = "completed"
                break

    # Format live dialer status payload for UI polling
    def get_status_payload(self) -> Dict[str, Any]:
        kpi = self.get_kpi_counts()
        next_lead = next((i for i in self.queue if i["status"] == "pending"), None)

        active_list: List[Dict[str, Any]] = []
        if self.active_call:
            uuid = self.active_call.get("call_uuid", "")
            doc = db.get_call(uuid) if uuid else None
            turns = doc.get("turns", []) if doc else []
            elapsed = int(time.time() - self.active_call.get("started_at", time.time()))

            ac_obj = {
                "call_uuid": uuid,
                "phone_number": self.active_call.get("phone_number", ""),
                "customer_name": self.active_call.get("customer_name", ""),
                "status": self.active_call.get("status", "in_call"),
                "elapsed_sec": max(0, elapsed),
                "started_at": self.active_call.get("started_at"),
                "turns": turns,
                "transcripts": turns,
            }
            active_list.append(ac_obj)

        recent = db.get_recent_calls(1)
        last_rec = recent[0] if recent else None
        last_completed = None
        if self.last_call:
            last_completed = self.last_call
        elif last_rec:
            last_completed = {
                "call_uuid": last_rec.get("call_uuid", ""),
                "phone_number": last_rec.get("phone_number", ""),
                "customer_name": last_rec.get("customer_name") or "",
                "disposition": last_rec.get("disposition", "completed"),
                "duration": last_rec.get("duration", 0),
                "ended_at": last_rec.get("ended_at_epoch") or time.time(),
                "turns": last_rec.get("turns", []),
                "diagnostic": last_rec.get("diagnostic", {}),
            }

        return {
            "status": self.status,
            "is_running": self.status == "running",
            "is_paused": self.status == "paused",
            "running": self.status == "running",
            "paused": self.status == "paused",
            "stats": kpi,
            "kpis": kpi,
            "counts": kpi,
            "channels_active": len(active_list),
            "total_queued": len(self.queue),
            "pending_count": kpi["pending"],
            "active_calls": active_list,
            "active_call": active_list[0] if active_list else None,
            "last_completed_call": last_completed,
            "last_call": last_completed,
            "next_call": next_lead,
            "next_in_queue": next_lead,
            "did": os.getenv("FROM_NUMBER", ""),
        }

# Generate compact ISO-like timestamp string
def datetime_now_str() -> str:
    return time.strftime("%Y%m%d-%H%M%S")

dialer = OutboundDialer()
