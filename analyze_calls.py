import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

log_path = Path(__file__).resolve().parent / "logs" / "call_diagnostics.jsonl"
lines = log_path.read_text(encoding="utf-8").strip().splitlines()

calls_by_id = {}
for line in lines:
    if not line.strip():
        continue
    try:
        data = json.loads(line)
        calls_by_id[data["call_uuid"]] = data
    except Exception:
        pass

last_5 = list(calls_by_id.values())[-5:]
print(f"Total unique calls recorded: {len(calls_by_id)}")

for idx, call in enumerate(last_5, 1):
    uuid = call.get("call_uuid")
    phone = call.get("phone_number")
    diag = call.get("diagnostic", {})
    events = call.get("events", [])
    ev_map = {e["event"]: e["time_epoch"] for e in events if "event" in e and "time_epoch" in e}
    
    t_answer = ev_map.get("call_answered")
    t_stream = ev_map.get("vobiz_stream_connected")
    t_status = ev_map.get("vobiz_stream_status")
    t_gemini = ev_map.get("gemini_session_ready")
    t_cust_audio = ev_map.get("customer_audio_received")
    t_audio_out = ev_map.get("audio_forwarded_to_vobiz")
    t_ended = ev_map.get("call_ended") or ev_map.get("websocket_closed")
    
    greeting_latency = round(t_audio_out - t_answer, 2) if (t_audio_out and t_answer) else None
    stream_latency = round(t_stream - t_answer, 2) if (t_stream and t_answer) else None
    status_latency = round(t_status - t_answer, 2) if (t_status and t_answer) else None
    gemini_gen_latency = round(t_audio_out - t_gemini, 2) if (t_audio_out and t_gemini) else None
    total_duration = round(t_ended - t_answer, 2) if (t_ended and t_answer) else None
    
    turn_events = [e for e in events if e.get("event") in ("agent_turn_completed", "customer_turn_completed")]
    
    print("=" * 70)
    print(f"Call #{idx} | UUID: {uuid}")
    print(f"Phone: {phone} | Classification: {diag.get('classification')}")
    print(f"Summary: {diag.get('root_cause_summary')}")
    print(f"Audio: Inbound={diag.get('audio_metrics', {}).get('inbound_bytes')}B ({diag.get('audio_metrics', {}).get('inbound_chunks')} chunks) | Outbound={diag.get('audio_metrics', {}).get('outbound_bytes')}B ({diag.get('audio_metrics', {}).get('outbound_chunks')} chunks)")
    print(f"Total Call Duration: {total_duration}s | Conversation Turns: {len(turn_events)}")
    print(f"Hangup Cause: {diag.get('hangup', {}).get('cause')} | Source: {diag.get('hangup', {}).get('source')} | WS Close Code: {diag.get('websocket', {}).get('close_code')}")
    print("Latency Breakdown:")
    print(f"  - Answer to Stream Connected: {stream_latency}s")
    print(f"  - Gemini Ready to Greeting Audio (Generation): {gemini_gen_latency}s")
    print(f"  - Total Answer to Greeting: {greeting_latency}s")
    print("Key Events:")
    for ev in events:
        ts = ev.get("timestamp", "")
        short_ts = ts.split("T")[1][:12] if "T" in ts else ts
        detail_str = str(ev.get("details"))
        if len(detail_str) > 70:
            detail_str = detail_str[:70] + "..."
        print(f"    [{short_ts}] {ev.get('event')}: {detail_str}")
