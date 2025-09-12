import subprocess
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading
import time
import json
import base64

KAFKA_SERVER = "localhost:9094"
CHUNK_SIZE = 4096  # bytes (used only for audio)

def create_kafka_producer():
    """Retry KafkaProducer connection until Kafka is ready."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka.", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Kafka not available. Retrying in 2 seconds...", flush=True)
            time.sleep(2)

def stream_audio_to_kafka(topic: str, ffmpeg_cmd: list):
    producer = create_kafka_producer()
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def log_stderr():
        for line in process.stderr:
            line = line.decode(errors="ignore").strip()
            if line:
                print(f"[FFmpeg:{topic}] {line}", flush=True)

    threading.Thread(target=log_stderr, daemon=True).start()
    print(f"🎙️ Streaming {topic} to Kafka ...", flush=True)

    try:
        while True:
            chunk = process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            message = {
                "timestamp": time.time(),
                "data": base64.b64encode(chunk).decode("utf-8"),
            }
            producer.send(topic, message)

            print(f"[Producer] Sent audio chunk (size={len(chunk)})", flush=True)

    finally:
        process.stdout.close()
        process.wait()
        producer.flush()
        print(f"Finished streaming {topic}", flush=True)

def stream_video_frames(topic: str, ffmpeg_cmd: list):
    producer = create_kafka_producer()
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def log_stderr():
        for line in process.stderr:
            line = line.decode(errors="ignore").strip()
            if line:
                print(f"[FFmpeg:{topic}] {line}", flush=True)

    threading.Thread(target=log_stderr, daemon=True).start()
    print(f"Streaming {topic} to Kafka ...", flush=True)

    frame_id = 0
    buffer = b""

    try:
        while True:
            chunk = process.stdout.read(4096)
            if not chunk:
                break

            buffer += chunk
            start = buffer.find(b"\xff\xd8")  # JPEG SOI
            end = buffer.find(b"\xff\xd9")    # JPEG EOI

            while start != -1 and end != -1 and end > start:
                jpeg_bytes = buffer[start:end+2]
                buffer = buffer[end+2:]

                frame_id += 1
                message = {
                    "frame_id": frame_id,
                    "timestamp": time.time(),
                    "frame": base64.b64encode(jpeg_bytes).decode("utf-8"),
                }

                producer.send(topic, message)
                print(f"[Producer] Sent frame {frame_id} ({len(jpeg_bytes)} bytes)", flush=True)

                start = buffer.find(b"\xff\xd8")
                end = buffer.find(b"\xff\xd9")

    finally:
        process.stdout.close()
        process.wait()
        producer.flush()
        print(f"Finished streaming {topic}", flush=True)


if __name__ == "__main__":
    print("Starting the producer...", flush=True)

    # Use avfoundation devices on macOS
    # Check devices with: ffmpeg -f avfoundation -list_devices true -i ""

    # AUDIO: default mic (device 0)
    audio_cmd = [
        "ffmpeg", "-f", "avfoundation", "-i", ":0",
        "-f", "s16le", "-acodec", "pcm_s16le",
        "-ac", "1", "-ar", "16000",
        "-vn", "-loglevel", "warning", "-"
    ]

    # VIDEO: default webcam (device 0)
    video_cmd = [
        "ffmpeg", "-f", "avfoundation",
        "-framerate", "30", "-video_size", "1280x720",
        "-pixel_format", "uyvy422",
        "-i", "0:none",               # FaceTime HD Camera, no audio
        "-vf", "fps=1",               # downsample to 1 frame per second
        "-f", "image2pipe", "-qscale:v", "2",
        "-vcodec", "mjpeg", "-loglevel", "warning", "-"
    ]

    t1 = threading.Thread(target=stream_audio_to_kafka, args=("audio-stream", audio_cmd))
    t2 = threading.Thread(target=stream_video_frames, args=("video-stream", video_cmd))
    t1.start(); t2.start()
    t1.join(); t2.join()

    print("Done streaming audio and video.", flush=True)
