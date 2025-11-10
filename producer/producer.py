import subprocess
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading
import time
import json
import base64
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_SERVER = "kafka:9092"
VIDEO_FILE = "/input/pesu_sds.mp4"
CHUNK_SIZE = 4096  # bytes (used only for audio)


TOPICS = ["audio-stream", "video-stream", "diagram-detections", "ocr-sentences", "audio-transcripts", "summarizer"]

def clear_kafka_topics():
    """Delete and recreate topics to clear them."""
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    try:
        admin.delete_topics(TOPICS)
        print(f"[Kafka Admin] Deleted topics: {TOPICS}", flush=True)
        # Small delay to ensure topics are deleted
        time.sleep(2)
    except Exception as e:
        print(f"[Kafka Admin] Warning: could not delete topics: {e}", flush=True)
    
    # Recreate topics
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in TOPICS]
    try:
        admin.create_topics(new_topics)
        print(f"[Kafka Admin] Recreated topics: {TOPICS}", flush=True)
    except TopicAlreadyExistsError:
        print("[Kafka Admin] Topics already exist, continuing...", flush=True)
    finally:
        admin.close()
        
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
    """Stream audio chunks to Kafka (unchanged)."""
    producer = create_kafka_producer()
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def log_stderr():
        for line in process.stderr:
            line = line.decode(errors="ignore").strip()
            if line:
                print(f"[FFmpeg:{topic}] {line}", flush=True)

    threading.Thread(target=log_stderr, daemon=True).start()
    print(f"Streaming {topic} to Kafka ...", flush=True)

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
    """Extract video frames as JPEGs and stream to Kafka as JSON messages."""
    producer = create_kafka_producer()
    process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def log_stderr():
        for line in process.stderr:
            line = line.decode(errors="ignore").strip()
            if line:
                print(f"[FFmpeg:{topic}] {line}", flush=True)

    threading.Thread(target=log_stderr, daemon=True).start()
    print(f"🎥 Streaming {topic} to Kafka ...", flush=True)

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

                # Send JSON message
                producer.send(topic, message)
                print(f"[Producer] Sent frame {frame_id} ({len(jpeg_bytes)} bytes)", flush=True)

                start = buffer.find(b"\xff\xd8")
                end = buffer.find(b"\xff\xd9")

    finally:
        process.stdout.close()
        process.wait()
        producer.flush()
        print(f"🏁 Finished streaming {topic}", flush=True)
        
def send_stop_signal():
    producer = create_kafka_producer()
    stop_msg = {"type": "STOP", "timestamp": time.time()}
    topics = ["audio-stream", "video-stream", "diagram-detections", "ocr-sentences", "audio-transcripts"]
    for topic in topics:
        producer.send(topic, stop_msg)
        print(f"[STOP] Sent stop signal to {topic}", flush=True)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    print("Clearing Kafka topics before streaming...", flush=True)
    clear_kafka_topics()
    
    print("Starting the producer...", flush=True)

    # AUDIO: unchanged
    audio_cmd = [
        "ffmpeg", "-re", "-i", VIDEO_FILE,
        "-f", "s16le", "-acodec", "pcm_s16le",
        "-ac", "1", "-ar", "16000",
        "-vn", "-loglevel", "warning", "-"
    ]

    # VIDEO: output frames as JPEG
    video_cmd = [
        "ffmpeg", "-re", "-i", VIDEO_FILE,
        "-vf", "fps=1",  # adjust FPS (1 frame/sec here, can increase)
        "-f", "image2pipe", "-qscale:v", "2",
        "-vcodec", "mjpeg", "-loglevel", "warning", "-"
    ]

    t1 = threading.Thread(target=stream_audio_to_kafka, args=("audio-stream", audio_cmd))
    t2 = threading.Thread(target=stream_video_frames, args=("video-stream", video_cmd))
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    # print("Sending stop signal to all consumers...", flush=True)
    # send_stop_signal()

    print("Done streaming audio and video.", flush=True)