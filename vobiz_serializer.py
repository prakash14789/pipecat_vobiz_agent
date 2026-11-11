import json
import time
from typing import Optional
from loguru import logger
import diagnostics

from pipecat.frames.frames import AudioRawFrame, Frame, InputAudioRawFrame, InterruptionFrame
from pipecat.serializers.vobiz import (
    VobizFrameSerializer as BaseVobizFrameSerializer,
    parse_vobiz_start,
)

# Serializer extending official pipecat-vobiz with diagnostic milestone tracing and acoustic echo protection
class VobizFrameSerializer(BaseVobizFrameSerializer):
    def __init__(
        self,
        stream_id: str = "",
        call_id: Optional[str] = None,
        auth_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        params: Optional[BaseVobizFrameSerializer.InputParams] = None,
    ):
        super().__init__(
            stream_id=stream_id,
            call_id=call_id,
            auth_id=auth_id,
            auth_token=auth_token,
            params=params,
        )
        self._created_time = time.monotonic()
        self._last_audio_out_time = 0.0
        self._speech_turn_start = 0.0
        # Protect opening greeting from any interruption for 4.5 seconds
        self._greeting_protection_until = self._created_time + 4.5

    # Set call identifiers dynamically
    def set_call_identifiers(self, call_uuid: str, stream_id: Optional[str] = None) -> None:
        self._call_id = call_uuid
        self._created_time = time.monotonic()
        self._greeting_protection_until = self._created_time + 4.5
        if stream_id:
            self._stream_id = stream_id

    # Serialize frames to Vobiz JSON with outbound audio diagnostic tracking & echo cancellation protection
    async def serialize(self, frame: Frame) -> str | bytes | None:
        now = time.monotonic()
        if isinstance(frame, AudioRawFrame):
            data = frame.audio
            if data and self._call_id:
                # If audio was silent for > 400ms, mark this as start of a new speech turn
                if (now - self._last_audio_out_time) > 0.4:
                    self._speech_turn_start = now
                self._last_audio_out_time = now
                diagnostics.get_tracer(self._call_id).record_audio_out(
                    len(data), sample_rate=self._output_sample_rate or self._vobiz_sample_rate
                )

        elif isinstance(frame, InterruptionFrame):
            call_prefix = f"[{self._call_id[:8]}] " if self._call_id else ""
            # 1. Ignore interruptions during opening greeting
            if now < self._greeting_protection_until:
                elapsed = now - self._created_time
                logger.info(f"{call_prefix}Suppressing interruption during opening greeting ({elapsed:.2f}s < 4.5s)")
                return None

            # 2. Ignore interruptions triggered within 1.0s of agent starting to speak (acoustic echo from caller speaker)
            if self._speech_turn_start > 0 and (now - self._speech_turn_start) < 1.0:
                turn_elapsed = now - self._speech_turn_start
                logger.info(f"{call_prefix}Suppressing echo-induced interruption ({turn_elapsed:.2f}s into speech turn)")
                return None

            logger.info(f"{call_prefix}Legitimate caller interruption: sending clearAudio to Vobiz")
            answer = {"event": "clearAudio", "streamId": self._stream_id}
            return json.dumps(answer)

        return await super().serialize(frame)

    # Deserialize Vobiz JSON into Pipecat frames with inbound audio diagnostic tracking
    async def deserialize(self, data: str | bytes) -> Frame | None:
        if isinstance(data, str) and ("start" in data):
            try:
                msg = json.loads(data)
                if msg.get("event") == "start":
                    start_data = msg.get("start") or {}
                    st_id = msg.get("streamId") or start_data.get("streamId")
                    if st_id:
                        self._stream_id = st_id
                    if self._call_id:
                        diagnostics.get_tracer(self._call_id).record_event(
                            "vobiz_stream_connected",
                            {
                                "stream_id": self._stream_id,
                                "encoding": self._encoding,
                                "sample_rate": self._vobiz_sample_rate,
                            },
                        )
            except Exception:
                pass

        frame = await super().deserialize(data)
        if isinstance(frame, InputAudioRawFrame):
            if frame.audio and self._call_id:
                diagnostics.get_tracer(self._call_id).record_audio_in(len(frame.audio))
        return frame
