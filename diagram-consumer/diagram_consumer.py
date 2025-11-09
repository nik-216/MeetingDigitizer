import os
import cv2
import numpy as np
import json
import base64
import time
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from paddleocr import PaddleOCR
from transformers import BlipProcessor, BlipForConditionalGeneration
from PIL import Image
import torch
from ultralytics import YOLO

# ---------------- Kafka Settings ----------------
# KAFKA_SERVER = "kafka:9092"
KAFKA_SERVER = "localhost:9094"
INPUT_TOPIC = "video-stream"
OUTPUT_TOPIC = "diagram-detections"

# ---------------- Output Directories ----------------
OUTPUT_DIR = "diagram-output"
DEBUG_DIR = "debug_diagrams"
RAW_DIR = "raw_messages"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# ---------------- Initialize OCR ----------------
ocr = PaddleOCR(lang='en', show_log=False, use_angle_cls=False)

# ---------------- Load BLIP Model (CPU) ----------------
device = "cpu"  # Force CPU to prevent CUDA segfaults
LOCAL_BLIP_PATH = "./blip-model"  # path to the downloaded folder

blip_processor = BlipProcessor.from_pretrained(LOCAL_BLIP_PATH)
blip_model = BlipForConditionalGeneration.from_pretrained(LOCAL_BLIP_PATH).to(device)


# ---------------- Load YOLO Segmentation Model (CPU) ----------------
yolo_seg = YOLO("yolov8x-seg.pt")

# ---------------- Kafka Producer ----------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

# ---------------- Diagram Detection Classes ----------------
class DiagramDetector:
    def __init__(self, min_area=5000, min_aspect_ratio=0.3, max_aspect_ratio=3.0):
        self.min_area = min_area
        self.min_aspect_ratio = min_aspect_ratio
        self.max_aspect_ratio = max_aspect_ratio
    
    def detect_diagram_regions(self, frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        adaptive_thresh = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        contours, _ = cv2.findContours(adaptive_thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        diagram_regions = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area > self.min_area:
                x, y, w, h = cv2.boundingRect(contour)
                aspect_ratio = w / h if h > 0 else 0
                if self.min_aspect_ratio <= aspect_ratio <= self.max_aspect_ratio:
                    diagram_regions.append({
                        'bbox': (x, y, w, h),
                        'area': area,
                        'aspect_ratio': aspect_ratio,
                        'contour': contour
                    })
        return diagram_regions

    def classify_diagram_type(self, region_image):
        gray = cv2.cvtColor(region_image, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        edges = cv2.Canny(gray, 50, 150)
        edge_density = np.sum(edges > 0) / (h * w)
        lines = cv2.HoughLines(edges, 1, np.pi/180, threshold=50)
        line_count = len(lines) if lines is not None else 0
        circles = cv2.HoughCircles(
            gray, cv2.HOUGH_GRADIENT, 1, 20,
            param1=50, param2=30, minRadius=5, maxRadius=50
        )
        circle_count = len(circles[0]) if circles is not None else 0
        if circle_count >= 2:
            return "scatter_plot"
        elif circle_count >= 1:
            return "pie_chart"
        elif line_count >= 5 and edge_density > 0.05:
            return "chart_or_graph"
        elif edge_density > 0.1:
            return "complex_diagram"
        else:
            return "simple_diagram"

class DiagramDuplicateFilter:
    def __init__(self, time_window=10.0, similarity_threshold=0.85, max_history=50):
        self.time_window = time_window
        self.similarity_threshold = similarity_threshold
        self.max_history = max_history
        self.recent_detections = deque(maxlen=max_history)

    def is_duplicate(self, diagram_type, bbox, timestamp):
        cutoff_time = timestamp - self.time_window
        while self.recent_detections and self.recent_detections[0]['timestamp'] < cutoff_time:
            self.recent_detections.popleft()
        for detection in self.recent_detections:
            if detection['diagram_type'] == diagram_type:
                x1, y1, w1, h1 = bbox
                x2, y2, w2, h2 = detection['bbox']
                overlap_x = max(0, min(x1+w1, x2+w2) - max(x1, x2))
                overlap_y = max(0, min(y1+h1, y2+h2) - max(y1, y2))
                overlap_area = overlap_x * overlap_y
                area1, area2 = w1*h1, w2*h2
                union_area = area1 + area2 - overlap_area
                if union_area > 0:
                    overlap_ratio = overlap_area / union_area
                    if overlap_ratio > 0.5:
                        return True, detection, overlap_ratio
        return False, None, 0.0

    def add_detection(self, diagram_type, bbox, timestamp, frame_id):
        self.recent_detections.append({
            'diagram_type': diagram_type,
            'bbox': bbox,
            'timestamp': timestamp,
            'frame_id': frame_id
        })

diagram_detector = DiagramDetector()
duplicate_filter = DiagramDuplicateFilter()

# ---------------- Kafka Sending ----------------
def send_diagram_to_kafka(diagram_data, frame_id, diagram_crop):
    try:
        _, buffer = cv2.imencode(".jpg", diagram_crop)
        diagram_b64 = base64.b64encode(buffer).decode("utf-8")
        message = {
            'frame_id': frame_id,
            'diagram_type': diagram_data['type'],
            'bbox': diagram_data['bbox'],
            'area': diagram_data['area'],
            'aspect_ratio': diagram_data['aspect_ratio'],
            'extracted_text': diagram_data.get('text', []),
            'confidence': diagram_data.get('confidence', 0.8),
            'inference': diagram_data.get('inference', ""),   # 👈 FIXED HERE
            'detection_timestamp': time.time(),
            'readable_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'source': 'diagram_detector',
            'diagram_image': diagram_b64
        }
        message_key = f"frame_{frame_id}_diagram_{diagram_data['type']}"
        future = producer.send(OUTPUT_TOPIC, key=message_key, value=message)
        record_metadata = future.get(timeout=1)
        print(f"Sent diagram {diagram_data['type']} (frame {frame_id}) Partition {record_metadata.partition}, offset {record_metadata.offset}")
        return True
    except Exception as e:
        print(f"KAFKA SEND FAILED for frame {frame_id}: {e}")
        return False

# ---------------- BLIP Inference ----------------
def infer_diagram_local(crop_bgr) -> str:
    try:
        crop_rgb = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2RGB)
        pil_img = Image.fromarray(crop_rgb).resize((384, 384))  # resize for safety
        inputs = blip_processor(pil_img, return_tensors="pt").to(device)
        out = blip_model.generate(**inputs, max_new_tokens=50)
        caption = blip_processor.decode(out[0], skip_special_tokens=True)
        torch.cuda.empty_cache()
        return caption
    except Exception as e:
        print(f"BLIP inference failed: {e}")
        return ""

# ---------------- OCR ----------------
def extract_text_from_diagram(diagram_region):
    try:
        result = ocr.ocr(diagram_region)
        texts = []
        if result and result[0]:
            for detection in result[0]:
                if len(detection) >= 2:
                    text_info = detection[1]
                    if isinstance(text_info, (tuple, list)) and len(text_info) >= 2:
                        text, confidence = text_info[0], text_info[1]
                        if confidence > 0.5:
                            texts.append({'text': text, 'confidence': confidence})
        return texts
    except Exception as e:
        print(f"OCR failed: {e}")
        return []

# ---------------- Decode Kafka Frame ----------------
def decode_frame(message_value, frame_id):
    raw_path = f"{RAW_DIR}/msg_{frame_id}.bin"
    with open(raw_path, "wb") as f:
        f.write(message_value)
    try:
        data = json.loads(message_value.decode("utf-8"))
        if "frame" in data:
            frame_bytes = base64.b64decode(data["frame"])
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            timestamp = data.get("timestamp", None)
            return frame, timestamp
    except:
        pass
    try:
        frame = cv2.imdecode(np.frombuffer(message_value, np.uint8), cv2.IMREAD_COLOR)
        return frame, None
    except:
        return None, None

# ---------------- Mask Persons ----------------
def mask_persons(image):
    try:
        results = yolo_seg(image, conf=0.5)
        mask = np.zeros(image.shape[:2], dtype=np.uint8)
        for r in results:
            if r.masks is None:
                continue
            for cls, m in zip(r.boxes.cls, r.masks.xy):
                if int(cls) == 0:  # person
                    poly = np.array(m, dtype=np.int32)
                    cv2.fillPoly(mask, [poly], 255)
        masked_image = image.copy()
        masked_image[mask == 255] = 0
        return masked_image, mask
    except Exception as e:
        print(f"YOLO segmentation failed: {e}")
        return image.copy(), np.zeros(image.shape[:2], dtype=np.uint8)

# ---------------- Process Each Frame ----------------
def process_frame(message_value, frame_id):
    try:
        frame, timestamp = decode_frame(message_value, frame_id)
        if frame is None:
            return
        if timestamp is None:
            timestamp = time.time()

        frame_masked, person_mask = mask_persons(frame)
        diagram_regions = diagram_detector.detect_diagram_regions(frame_masked)
        detected_diagrams = []

        for region in diagram_regions:
            x, y, w, h = region['bbox']
            if np.any(person_mask[y:y+h, x:x+w] > 0):
                continue
            diagram_crop = frame_masked[y:y+h, x:x+w]
            diagram_type = diagram_detector.classify_diagram_type(diagram_crop)
            is_dup, _, _ = duplicate_filter.is_duplicate(diagram_type, region['bbox'], timestamp)
            if is_dup:
                continue
            extracted_text = extract_text_from_diagram(diagram_crop)
            blip_caption = infer_diagram_local(diagram_crop)
            diagram_data = {
                'type': diagram_type,
                'bbox': region['bbox'],
                'area': region['area'],
                'aspect_ratio': region['aspect_ratio'],
                'text': extracted_text,
                'confidence': min(1.0, len(extracted_text)*0.1 + 0.7),
                'inference': blip_caption
            }
            print(diagram_data)
            detected_diagrams.append((diagram_data, diagram_crop))
            duplicate_filter.add_detection(diagram_type, region['bbox'], timestamp, frame_id)

        for diagram_data, crop in detected_diagrams:
            send_diagram_to_kafka(diagram_data, frame_id, crop)

    except Exception as e:
        print(f"Error processing frame {frame_id}: {e}")

# ---------------- Kafka Consumer ----------------
def consume_frames():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="diagram-detector",
        value_deserializer=lambda m: m
    )
    print("Listening for frames...")
    for i, message in enumerate(consumer):
        process_frame(message.value, i)

# ---------------- Main ----------------
if __name__ == "__main__":
    try:
        consume_frames()
    except KeyboardInterrupt:
        producer.close()
