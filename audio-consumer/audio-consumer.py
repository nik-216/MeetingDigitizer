import json
import base64
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from resemblyzer import VoiceEncoder
from faster_whisper import WhisperModel
from kafka.admin import KafkaAdminClient, NewTopic
import time

# Topics
INPUT_TOPIC = "audio-stream"
OUTPUT_TOPIC = "audio-transcripts"
BOOTSTRAP_SERVERS = "kafka:9092"

# Audio params
SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # s16le
CHANNELS = 1
WINDOW_SECONDS = 5.0
WINDOW_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * WINDOW_SECONDS)

# Embedding params
EMB_WIN_SEC = 1.5
EMB_HOP_SEC = 0.75

# Models
encoder = VoiceEncoder()  # CPU by default
whisper_model = WhisperModel("base", device="cpu")  # or "cuda"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def pcm_bytes_to_float32(pcm_bytes: bytes) -> np.ndarray:
    x = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
    if CHANNELS > 1:
        x = x.reshape(-1, CHANNELS).mean(axis=1)
    x /= 32768.0
    return x


def frame_audio(wav: np.ndarray, sr: int, win_sec: float, hop_sec: float):
    win = int(win_sec * sr)
    hop = int(hop_sec * sr)
    for start in range(0, max(1, len(wav) - win + 1), hop):
        end = start + win
        if end <= len(wav):
            yield start / sr, end / sr, wav[start:end]


def diarize(wav16k: np.ndarray, num_speakers: int = 2):
    """
    Real diarization with fixed number of speakers and merged segments.
    Returns list of (start_s, end_s, spk).
    """
    frames = list(frame_audio(wav16k, SAMPLE_RATE, EMB_WIN_SEC, EMB_HOP_SEC))
    if not frames:
        return []

    embs = []
    times = []
    for s, e, seg in frames:
        embs.append(encoder.embed_utterance(seg))
        times.append((s, e))
    embs = np.vstack(embs)

    # Force exactly num_speakers clusters
    from sklearn.cluster import KMeans
    km = KMeans(n_clusters=num_speakers, n_init=10, random_state=0)
    labels = km.fit_predict(embs)

    # Merge consecutive frames of same speaker
    diar_segments = []
    prev_lab = labels[0]
    seg_start, seg_end = times[0]

    for (s, e), lab in zip(times[1:], labels[1:]):
        if lab == prev_lab:
            seg_end = e  # extend segment
        else:
            diar_segments.append((seg_start, seg_end, f"spk{prev_lab+1}"))
            seg_start, seg_end = s, e
            prev_lab = lab

    diar_segments.append((seg_start, seg_end, f"spk{prev_lab+1}"))
    return diar_segments


def assign_speakers_to_asr_segments(diar, asr_segments):
    """
    Assign speakers to ASR segments based on merged diarization.
    Each ASR segment is labeled by the speaker whose segment it overlaps the most.
    """
    if not diar:
        return [("spk?", s) for s in asr_segments]

    out = []
    for seg in asr_segments:
        # Compute overlap with each diar segment
        best_overlap = 0
        assigned_spk = "spk?"
        for s, e, spk in diar:
            overlap = max(0, min(e, seg.end) - max(s, seg.start))
            if overlap > best_overlap:
                best_overlap = overlap
                assigned_spk = spk
        out.append((assigned_spk, seg))
    return out


def process_window(pcm_chunk: bytes, kafka_timestamp: float):
    wav = pcm_bytes_to_float32(pcm_chunk)
    diar = diarize(wav)

    segments, _ = whisper_model.transcribe(wav, beam_size=5)
    tagged = assign_speakers_to_asr_segments(diar, list(segments))

    for spk, seg in tagged:
        text = (seg.text or "").strip()
        if text:
            msg = {
                "kafka_timestamp": kafka_timestamp,
                "start": seg.start,
                "end": seg.end,
                "speaker": spk,
                "text": text,
            }

            # Print locally
            print(
                f"[ASR] ts={kafka_timestamp:.6f} {spk} "
                f"{seg.start:.2f}-{seg.end:.2f}s: {text}",
                flush=True,
            )

            # Send to Kafka output topic
            producer.send(OUTPUT_TOPIC, msg)


def _parse_payload(raw):
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode("utf-8"))
    if isinstance(raw, dict):
        return raw
    raise TypeError(f"Unexpected message.value type: {type(raw)}")


def send_completed_signal():
    """Send a completion signal to the audio-transcripts Kafka topic."""
    try:
        completed_message = {
            "type": "COMPLETED",
            "timestamp": time.time(),
            "readable_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            "source": "audio_consumer"
        }

        producer.send(
            OUTPUT_TOPIC,
            key="completed_signal".encode('utf-8'),
            value=completed_message
        )
        producer.flush()
        print(f"[COMPLETED] Sent completion signal to {OUTPUT_TOPIC}", flush=True)

    except Exception as e:
        print(f"[COMPLETED] Failed to send completion signal: {e}", flush=True)


def clear_topic(topic_name: str):
    """Delete and recreate a Kafka topic to clear its messages."""
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=[BOOTSTRAP_SERVERS])
            try:
                admin.delete_topics([topic_name])
                print(f"[CLEAR] Deleted topic: {topic_name}", flush=True)
            except Exception as e:
                print(f"[CLEAR] Could not delete {topic_name} (maybe doesn't exist): {e}", flush=True)
            try:
                admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
                print(f"[CLEAR] Recreated topic: {topic_name}", flush=True)
            except Exception as e:
                print(f"[CLEAR] Could not create {topic_name} (maybe already exists): {e}", flush=True)
            admin.close()
            break
        except Exception as e:
            print(f"[CLEAR] Admin error: {e}. Retrying in 2s...", flush=True)
            time.sleep(2)

def main():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        group_id="audio-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"[Audio Consumer] Listening on topic: {INPUT_TOPIC}", flush=True)

    buffer = bytearray()
    last_ts = None

    for msg in consumer:
        try:
            payload = _parse_payload(msg.value)

            # Detect STOP signal
            if isinstance(payload, dict) and payload.get("type") == "STOP":
                print(f"[STOP] Signal received on topic {INPUT_TOPIC}", flush=True)
                clear_topic(INPUT_TOPIC)
                send_completed_signal()
                break  # exit loop to shutdown

            # Normal audio processing
            audio_bytes = base64.b64decode(payload["data"])
            ts = float(payload["timestamp"])
            last_ts = ts

            buffer.extend(audio_bytes)

            while len(buffer) >= WINDOW_BYTES:
                window = bytes(buffer[:WINDOW_BYTES])
                del buffer[:WINDOW_BYTES]
                process_window(window, last_ts)

        except Exception as e:
            print(f"[Audio Consumer] Error: {e}", flush=True)

    # Cleanup
    print("[Audio Consumer] Shutting down...", flush=True)
    producer.close()
    consumer.close()
    print("[Audio Consumer] Producer and consumer closed successfully.", flush=True)

if __name__ == "__main__":
    main()
