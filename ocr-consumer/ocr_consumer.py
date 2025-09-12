import os
import cv2
import numpy as np
import json
import base64
import time
from collections import deque
from difflib import SequenceMatcher
from kafka import KafkaConsumer, KafkaProducer
from paddleocr import PaddleOCR

# Kafka settings
KAFKA_SERVER = "kafka:9092"
INPUT_TOPIC = "video-stream"
OUTPUT_TOPIC = "ocr-sentences"  # New topic for sentence output

# Output directories
OUTPUT_DIR = "ocr_output"
DEBUG_DIR = "debug_frames"
RAW_DIR = "raw_messages"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# Initialize OCR with simpler settings to avoid preprocessing issues
ocr = PaddleOCR(
    lang='en',
    show_log=False,
    use_angle_cls=False   # works with PaddleOCR <= 2.6
)

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def send_sentence_to_kafka(sentence_data, frame_id):
    """Send sentence data to Kafka topic and log to console"""
    try:
        # Prepare the message payload
        message = {
            'frame_id': frame_id,
            'sentence': sentence_data['text'],
            'confidence': sentence_data['confidence'],
            'word_count': sentence_data['word_count'],
            'line_number': sentence_data['line_number'],
            'detection_timestamp': time.time(),  # When the sentence was processed
            'readable_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'source': 'ocr_consumer'
        }
        
        # Create a unique key for the message
        message_key = f"frame_{frame_id}_line_{sentence_data['line_number']}"
        
        # Send to Kafka topic
        future = producer.send(
            OUTPUT_TOPIC,
            key=message_key,
            value=message
        )
        
        # Log successful send attempt
        print(f"KAFKA SEND -> Topic: {OUTPUT_TOPIC}")
        print(f"   Key: {message_key}")
        print(f"   Sentence: \"{sentence_data['text']}\"")
        print(f"   Confidence: {sentence_data['confidence']:.3f}")
        print(f"   Timestamp: {message['readable_time']}")
        print(f"   Frame ID: {frame_id}, Line: {sentence_data['line_number']}")
        
        # Optional: Wait for send confirmation (can be removed for better performance)
        try:
            record_metadata = future.get(timeout=1)
            print(f"   Sent successfully to partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as send_error:
            print(f"   Send confirmation failed: {send_error}")
        
        print("-" * 60)
        
        return True
        
    except Exception as e:
        print(f"KAFKA SEND FAILED for frame {frame_id}: {e}")
        import traceback
        traceback.print_exc()
        return False

def log_sentence_batch(sentences, frame_id, timestamp):
    """Log a summary of all sentences being sent"""
    if not sentences:
        return
        
    readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
    
    print(f"\nSENDING BATCH TO KAFKA:")
    print(f"Frame: {frame_id} | Time: {readable_time}")
    print(f"Sentences to send: {len(sentences)}")
    print("=" * 60)
    
    for i, sentence in enumerate(sentences, 1):
        print(f"[{i}] Line {sentence['line_number']}: \"{sentence['text']}\"")
        print(f"    Confidence: {sentence['confidence']:.3f}, Words: {sentence['word_count']}")
    
    print("=" * 60)

# Duplicate detection settings
class DuplicateFilter:
    def __init__(self, time_window=10.0, similarity_threshold=0.8, max_history=100):
        """
        Initialize duplicate filter
        
        Args:
            time_window: Time window in seconds to check for duplicates
            similarity_threshold: Text similarity threshold (0-1, higher = more strict)
            max_history: Maximum number of recent detections to keep
        """
        self.time_window = time_window
        self.similarity_threshold = similarity_threshold
        self.max_history = max_history
        self.recent_detections = deque(maxlen=max_history)
        
    def normalize_text(self, text):
        """Normalize text for better comparison by removing common OCR artifacts"""
        import re
        # Convert to lowercase and strip
        normalized = text.lower().strip()
        # Remove leading/trailing punctuation
        normalized = re.sub(r'^[^\w]+|[^\w]+$', '', normalized)
        # Remove extra spaces
        normalized = re.sub(r'\s+', ' ', normalized)
        # Remove common OCR artifacts
        normalized = re.sub(r'[^\w\s]', '', normalized)  # Remove all punctuation
        return normalized
    
    def calculate_similarity(self, text1, text2):
        """Calculate similarity between two texts using multiple methods"""
        # Normalize both texts
        norm1 = self.normalize_text(text1)
        norm2 = self.normalize_text(text2)
        
        # Method 1: Basic sequence matcher on normalized text
        basic_similarity = SequenceMatcher(None, norm1, norm2).ratio()
        
        # Method 2: Check if one text is a substring of the other (for truncated OCR)
        if len(norm1) > 0 and len(norm2) > 0:
            shorter = norm1 if len(norm1) < len(norm2) else norm2
            longer = norm2 if len(norm1) < len(norm2) else norm1
            
            if shorter in longer and len(shorter) >= 3:  # At least 3 characters
                substring_similarity = len(shorter) / len(longer)
                # Give high similarity if shorter text is significant portion of longer
                if substring_similarity > 0.7:
                    return max(basic_similarity, 0.9)
        
        # Method 3: Word-level similarity for multi-word texts
        words1 = norm1.split()
        words2 = norm2.split()
        
        if len(words1) > 1 and len(words2) > 1:
            # Calculate word overlap
            common_words = set(words1) & set(words2)
            total_words = len(set(words1) | set(words2))
            word_similarity = len(common_words) / total_words if total_words > 0 else 0
            
            # If most words match, give high similarity
            if word_similarity > 0.7:
                return max(basic_similarity, word_similarity)
        
        return basic_similarity
    
    def is_duplicate(self, text, timestamp):
        """Check if text is a duplicate of recent detections"""
        current_time = timestamp
        
        # Clean up old detections outside time window
        cutoff_time = current_time - self.time_window
        while self.recent_detections and self.recent_detections[0]['timestamp'] < cutoff_time:
            self.recent_detections.popleft()
        
        # Check similarity with recent detections
        for detection in self.recent_detections:
            similarity = self.calculate_similarity(text, detection['text'])
            if similarity >= self.similarity_threshold:
                return True, detection, similarity
        
        return False, None, 0.0
    
    def add_detection(self, text, timestamp, frame_id):
        """Add a new detection to the history"""
        self.recent_detections.append({
            'text': text,
            'timestamp': timestamp,
            'frame_id': frame_id
        })
    
    def get_stats(self):
        """Get statistics about current detections"""
        return {
            'total_recent_detections': len(self.recent_detections),
            'oldest_detection_age': time.time() - self.recent_detections[0]['timestamp'] if self.recent_detections else 0
        }

# Initialize global duplicate filter
duplicate_filter = DuplicateFilter(
    time_window=8.0,      # 8 second window (longer for video)
    similarity_threshold=0.75,  # Lower threshold to catch more variations
    max_history=100       # Keep more detections for video
)

def convert_to_json_serializable(obj):
    """Convert numpy types to JSON-serializable Python types"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj

def preprocess_frame(frame):
    """Preprocess frame for OCR with validation"""
    # Validate input frame
    if frame is None or frame.size == 0:
        print("Invalid frame for preprocessing")
        return None
    
    # Convert to grayscale
    if len(frame.shape) == 3:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    else:
        gray = frame
    
    # Validate grayscale conversion
    if gray is None or gray.size == 0:
        print("Grayscale conversion failed")
        return None
    
    # Apply threshold
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    # Validate threshold result
    if thresh is None or thresh.size == 0:
        print("Threshold operation failed")
        return None
    
    # Scale up (but ensure reasonable dimensions)
    h, w = thresh.shape
    if h < 32 or w < 32:
        print(f"Image too small: {w}x{h}")
        return None
    
    # Limit maximum size to prevent memory issues
    max_dim = 4000
    if h > max_dim or w > max_dim:
        scale = max_dim / max(h, w)
        new_w, new_h = int(w * scale), int(h * scale)
        scaled = cv2.resize(thresh, (new_w, new_h), interpolation=cv2.INTER_CUBIC)
    else:
        scaled = cv2.resize(thresh, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    
    # Final validation
    if scaled is None or scaled.size == 0:
        print("Scaling failed")
        return None
    
    return scaled

def draw_boxes(frame, ocr_results):
    """Draw bounding boxes and text on frame"""
    if not ocr_results:
        return frame
        
    for line in ocr_results:
        try:
            if len(line) >= 2:
                box = line[0]
                text_info = line[1]
                
                # Handle different text info formats
                if isinstance(text_info, tuple) and len(text_info) >= 2:
                    txt, conf = text_info[0], text_info[1]
                elif isinstance(text_info, list) and len(text_info) >= 2:
                    txt, conf = text_info[0], text_info[1]
                elif isinstance(text_info, str):
                    txt, conf = text_info, 1.0
                else:
                    print(f"Unexpected text_info format: {text_info}")
                    continue
                
                # Ensure box is valid numpy array
                try:
                    box = np.array(box, dtype=np.float32)
                    if box.shape[0] < 4 or box.shape[1] != 2:
                        print(f"Invalid box dimensions: {box.shape}")
                        continue
                        
                    box = box.astype(int)  # Convert to int for drawing
                    
                    # Draw bounding box
                    cv2.polylines(frame, [box], isClosed=True, color=(0, 255, 0), thickness=2)
                    cv2.putText(frame, f"{txt} ({conf:.2f})",
                               (box[0][0], box[0][1] - 10),
                               cv2.FONT_HERSHEY_SIMPLEX,
                               0.7, (255, 0, 0), 2)
                               
                except (ValueError, TypeError) as box_error:
                    print(f"Box conversion error: {box_error}, box: {box}")
                    continue
                    
        except Exception as e:
            print(f"Error drawing box for line: {e}")
            continue
    
    return frame

def decode_frame(message_value, frame_id):
    """Decode Kafka message -> OpenCV BGR image and extract timestamp"""
    # Save raw message for debugging
    raw_path = f"{RAW_DIR}/msg_{frame_id}.bin"
    with open(raw_path, "wb") as f:
        f.write(message_value)

    # Case 1: JSON with base64 (most likely)
    try:
        data = json.loads(message_value.decode("utf-8"))
        if "frame" in data:
            frame_bytes = base64.b64decode(data["frame"])
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            
            # Extract timestamp if available
            timestamp = data.get("timestamp", None)
            
            if frame is not None:
                return frame, timestamp
    except Exception as e:
        print(f"JSON decode failed for frame {frame_id}: {e}")

    # Case 2: raw JPEG bytes (fallback)
    try:
        frame = cv2.imdecode(np.frombuffer(message_value, np.uint8), cv2.IMREAD_COLOR)
        timestamp = None  # No timestamp available in raw bytes
        return frame, timestamp
    except Exception as e:
        print(f"Raw JPEG decode failed for frame {frame_id}: {e}")
        return None, None

def extract_ocr_data(result):
    """Extract OCR data from PaddleOCR result, handling different formats"""
    try:
        print(f"Debug - OCR result type: {type(result)}")
        
        # Handle new predict() format vs old ocr() format
        if isinstance(result, dict):
            print(f"Debug - Dict keys: {list(result.keys())}")
            
            # New format: result is a dict with keys like 'dt_polys', 'rec_texts', etc.
            if 'dt_polys' in result and 'rec_texts' in result:
                boxes = result['dt_polys']
                texts = result['rec_texts']
                scores = result.get('rec_scores', [1.0] * len(texts))
                
                print(f"Debug - Found {len(texts)} texts, {len(boxes)} boxes, {len(scores)} scores")
                
                # Combine into expected format: [[box, (text, score)], ...]
                ocr_data = []
                for i in range(min(len(texts), len(boxes), len(scores))):
                    ocr_data.append([boxes[i], (texts[i], scores[i])])
                
                return [ocr_data] if ocr_data else None  # Wrap in list to match old format
            else:
                print(f"Unknown dict format keys: {result.keys()}")
                return None
        
        elif isinstance(result, list):
            print(f"Debug - List format, length: {len(result)}")
            # Old format: result is already a list
            if len(result) > 0:
                print(f"Debug - First element type: {type(result[0])}")
                if isinstance(result[0], list) and len(result[0]) > 0:
                    print(f"Debug - First detection: {result[0][0]}")
            return result
        
        elif hasattr(result, '__iter__'):
            # Try to convert iterator to list
            print(f"Debug - Converting iterator to list")
            result_list = list(result)
            print(f"Debug - Iterator converted, length: {len(result_list)}")
            return result_list
        
        else:
            print(f"Unknown result type: {type(result)}")
            return None
            
    except Exception as e:
        print(f"Error extracting OCR data: {e}")
        import traceback
        traceback.print_exc()
        return None

def group_text_into_lines(ocr_result):
    """Group OCR results into logical text lines based on spatial proximity"""
    if not ocr_result or not ocr_result[0]:
        return []
    
    text_elements = []
    
    # Extract all text elements with their positions
    for detection in ocr_result[0]:
        try:
            if len(detection) < 2:
                continue
                
            box = detection[0]
            text_info = detection[1]
            
            # Handle different text_info formats
            if isinstance(text_info, (tuple, list)) and len(text_info) >= 2:
                txt, conf = text_info[0], text_info[1]
            elif isinstance(text_info, (tuple, list)) and len(text_info) == 1:
                txt, conf = text_info[0], 1.0
            elif isinstance(text_info, str):
                txt, conf = text_info, 1.0
            else:
                continue
            
            if not txt or not isinstance(txt, str) or not txt.strip():
                continue
            
            # Ensure box is a proper numpy array of coordinates
            box = np.array(box, dtype=np.float32)
            if box.shape[0] < 4 or box.shape[1] != 2:
                print(f"Invalid box shape: {box.shape}")
                continue
                
            # Calculate bounding box center and dimensions
            x_center = np.mean(box[:, 0])
            y_center = np.mean(box[:, 1])
            height = np.max(box[:, 1]) - np.min(box[:, 1])
            
            text_elements.append({
                'text': txt.strip(),
                'confidence': float(conf),  # Convert to Python float
                'x_center': float(x_center),  # Convert to Python float
                'y_center': float(y_center),  # Convert to Python float
                'height': float(height),  # Convert to Python float
                'box': box
            })
            
        except Exception as e:
            print(f"Error processing detection: {e}")
            continue
    
    if not text_elements:
        return []
    
    # Sort by vertical position (top to bottom)
    text_elements.sort(key=lambda x: x['y_center'])
    
    # Group into lines based on vertical proximity
    lines = []
    current_line = [text_elements[0]]
    
    for i in range(1, len(text_elements)):
        current = text_elements[i]
        previous = text_elements[i-1]
        
        # If elements are vertically close (within 1.5x height), they're on the same line
        vertical_threshold = max(current['height'], previous['height']) * 1.5
        
        if abs(current['y_center'] - previous['y_center']) <= vertical_threshold:
            current_line.append(current)
        else:
            # Sort current line by horizontal position (left to right)
            current_line.sort(key=lambda x: x['x_center'])
            lines.append(current_line)
            current_line = [current]
    
    # Don't forget the last line
    if current_line:
        current_line.sort(key=lambda x: x['x_center'])
        lines.append(current_line)
    
    return lines

def filter_duplicate_sentences(sentences, timestamp, frame_id):
    """Filter out duplicate sentences using the global duplicate filter"""
    filtered_sentences = []
    duplicate_info = []
    
    for sentence in sentences:
        text = sentence['text']
        is_dup, original_detection, similarity = duplicate_filter.is_duplicate(text, timestamp)
        
        if is_dup:
            duplicate_info.append({
                'text': text,
                'similarity': similarity,
                'original_frame': original_detection['frame_id'],
                'time_diff': timestamp - original_detection['timestamp']
            })
            print(f"Duplicate detected: \"{text[:50]}...\" (similarity: {similarity:.3f}, original frame: {original_detection['frame_id']})")
        else:
            # Additional check: look for very similar recent sentences (stricter for video)
            recent_similar = False
            for recent_detection in list(duplicate_filter.recent_detections)[-5:]:  # Check last 5 detections
                if recent_detection['frame_id'] != frame_id:  # Don't compare with same frame
                    recent_similarity = duplicate_filter.calculate_similarity(text, recent_detection['text'])
                    if recent_similarity > 0.90:  # Very high threshold for recent frames
                        recent_similar = True
                        duplicate_info.append({
                            'text': text,
                            'similarity': recent_similarity,
                            'original_frame': recent_detection['frame_id'],
                            'time_diff': timestamp - recent_detection['timestamp'],
                            'reason': 'high_recent_similarity'
                        })
                        print(f"High similarity to recent frame: \"{text[:50]}...\" (similarity: {recent_similarity:.3f}, frame: {recent_detection['frame_id']})")
                        break
            
            if not recent_similar:
                filtered_sentences.append(sentence)
                duplicate_filter.add_detection(text, timestamp, frame_id)
                print(f"New text: \"{text[:50]}...\"")
    
    return filtered_sentences, duplicate_info

def extract_sentences_and_words(ocr_result, timestamp):
    """Extract words and sentences with timestamps"""
    lines = group_text_into_lines(ocr_result)
    
    results = {
        'timestamp': float(timestamp) if timestamp else None,  # Convert to Python float
        'sentences': [],
        'words': [],
        'raw_detections': len(ocr_result[0]) if ocr_result and ocr_result[0] else 0
    }
    
    for line_idx, line_elements in enumerate(lines):
        # Combine words in line to form sentence
        words_in_line = []
        line_confidences = []
        
        for element in line_elements:
            word = element['text']
            conf = element['confidence']
            
            words_in_line.append(word)
            line_confidences.append(conf)
            
            # Add individual word
            results['words'].append({
                'text': word,
                'confidence': float(conf),  # Ensure Python float
                'position': {
                    'x': float(element['x_center']),  # Ensure Python float
                    'y': float(element['y_center'])   # Ensure Python float
                },
                'line_number': line_idx + 1
            })
        
        if words_in_line:
            sentence = ' '.join(words_in_line)
            avg_confidence = sum(line_confidences) / len(line_confidences)
            
            results['sentences'].append({
                'text': sentence,
                'confidence': float(avg_confidence),  # Ensure Python float
                'word_count': len(words_in_line),
                'line_number': line_idx + 1
            })
    
    return results

def process_frame(message_value, frame_id):
    try:
        frame, timestamp = decode_frame(message_value, frame_id)
        if frame is None:
            print(f"Frame {frame_id} could not be decoded")
            return

        # Use current time if no timestamp from message
        if timestamp is None:
            timestamp = time.time()

        # Validate frame dimensions and format
        if len(frame.shape) != 3 or frame.shape[2] != 3:
            print(f"Frame {frame_id} has invalid format: {frame.shape}")
            return

        h, w = frame.shape[:2]
        if h < 32 or w < 32:
            print(f"Frame {frame_id} too small: {w}x{h}")
            return

        # Resize if too big (before preprocessing)
        if h > 1080 or w > 1920:
            frame = cv2.resize(frame, (1920, 1080))
            print(f"Frame {frame_id} resized to 1920x1080")

        print(f"🔍 Frame {frame_id} using original image: {frame.shape}, dtype: {frame.dtype}")

        # Try OCR with error handling
        try:
            # Use original frame directly to avoid preprocessing issues
            try:
                print(f"Trying ocr() method on original frame {frame_id}")
                raw_result = ocr.ocr(frame)
                print(f"Raw result type: {type(raw_result)}")
                print(f"Raw result length: {len(raw_result) if hasattr(raw_result, '__len__') else 'no len'}")
                
                # Debug the actual structure
                if raw_result:
                    print(f"First element type: {type(raw_result[0])}")
                    if hasattr(raw_result[0], '__len__') and len(raw_result[0]) > 0:
                        print(f"First detection type: {type(raw_result[0][0])}")
                        print(f"First detection: {raw_result[0][0]}")
                
                result = raw_result  # ocr() should return the right format
                print(f"OCR (old method) completed for frame {frame_id}")
                
            except Exception as old_method_error:
                print(f"Old method failed: {old_method_error}")
                print(f"Skipping predict() method due to known issues")
                cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_ocr_failed.jpg", frame)
                return
            
        except Exception as ocr_error:
            print(f"OCR failed for frame {frame_id}: {ocr_error}")
            import traceback
            traceback.print_exc()
            cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_ocr_failed.jpg", frame)
            return

        # Debug the final result structure
        print(f"Final result type: {type(result)}")
        if result:
            print(f"Final result length: {len(result)}")
            if len(result) > 0 and result[0]:
                print(f"First result element type: {type(result[0])}")
                print(f"First result element length: {len(result[0])}")
                if len(result[0]) > 0:
                    print(f"First detection structure: {result[0][0]}")

        if not result or not result[0]:
            print(f"Frame {frame_id}: No text detected")
            cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_notext.jpg", frame)
            return

        # Extract sentences and words with timestamps
        text_analysis = extract_sentences_and_words(result, timestamp)
        
        # Filter duplicate sentences
        original_sentence_count = len(text_analysis['sentences'])
        filtered_sentences, duplicate_info = filter_duplicate_sentences(
            text_analysis['sentences'], timestamp, frame_id
        )
        
        # Update text analysis with filtered sentences
        text_analysis['sentences'] = filtered_sentences
        text_analysis['duplicate_info'] = duplicate_info
        text_analysis['duplicates_filtered'] = original_sentence_count - len(filtered_sentences)
        
        # Convert timestamp to readable format
        readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        
        print(f"\nFrame {frame_id} - {readable_time}")
        print(f"Detected {text_analysis['raw_detections']} text elements")
        print(f"Original sentences: {original_sentence_count}, After filtering: {len(filtered_sentences)} ({text_analysis['duplicates_filtered']} duplicates removed)")
        
        # Show duplicate filter stats
        filter_stats = duplicate_filter.get_stats()
        print(f"Filter stats: {filter_stats['total_recent_detections']} recent detections in memory")
        
        # Only proceed with output if we have new (non-duplicate) sentences
        if filtered_sentences:
            # Log sentence batch before sending
            log_sentence_batch(filtered_sentences, frame_id, timestamp)
            
            # Send each sentence to Kafka topic
            successful_sends = 0
            for sentence in filtered_sentences:
                if send_sentence_to_kafka(sentence, frame_id):
                    successful_sends += 1
            
            # Print sentences
            print("\nNEW SENTENCES DETECTED:")
            for i, sentence in enumerate(filtered_sentences, 1):
                print(f"  Line {sentence['line_number']}: \"{sentence['text']}\"")
                print(f"    ↳ Confidence: {sentence['confidence']:.2f}, Words: {sentence['word_count']}")
                
            print(f"\nKAFKA SUMMARY: {successful_sends}/{len(filtered_sentences)} sentences sent successfully")
            
        else:
            print("\nNo new sentences (all were duplicates)")
        
        # Show duplicate info if any
        if duplicate_info:
            print(f"\nDUPLICATES FILTERED ({len(duplicate_info)}):")
            for dup in duplicate_info:
                print(f"  \"{dup['text'][:50]}...\" (sim: {dup['similarity']:.2f}, from frame {dup['original_frame']})")
        
        # Print words (optional, can comment out if too verbose)
        if text_analysis['words'] and filtered_sentences:  # Only show words if we have new sentences
            print(f"\nINDIVIDUAL WORDS ({len(text_analysis['words'])}):")
            for word in text_analysis['words']:
                print(f"  \"{word['text']}\" (conf: {word['confidence']:.2f}, line: {word['line_number']})")

        # Convert output data to JSON-serializable format
        output_data = {
            'frame_id': frame_id,
            'timestamp': float(timestamp),
            'readable_time': readable_time,
            'sentences': text_analysis['sentences'],
            'words': text_analysis['words'],
            'raw_detections': text_analysis['raw_detections'],
            'duplicates_filtered': text_analysis['duplicates_filtered'],
            'duplicate_info': duplicate_info,
            'has_new_content': len(filtered_sentences) > 0
        }
        
        # Ensure all data is JSON serializable
        output_data = convert_to_json_serializable(output_data)
        
        # Always save JSON for debugging purposes, but mark if it has new content
        with open(f"{OUTPUT_DIR}/frame_{frame_id}.json", "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        # Only save detailed text output if there's new content
        if output_data['has_new_content']:
            # Save human-readable text
            with open(f"{OUTPUT_DIR}/frame_{frame_id}.txt", "w", encoding="utf-8") as f:
                f.write(f"Frame {frame_id} - {readable_time}\n")
                f.write("=" * 50 + "\n\n")
                
                if filtered_sentences:
                    f.write("NEW SENTENCES:\n")
                    for sentence in filtered_sentences:
                        f.write(f"Line {sentence['line_number']}: {sentence['text']}\n")
                    f.write("\n")
                
                if duplicate_info:
                    f.write("DUPLICATES FILTERED:\n")
                    for dup in duplicate_info:
                        f.write(f"  \"{dup['text']}\" (similarity: {dup['similarity']:.2f})\n")
                    f.write("\n")
                
                if text_analysis['words']:
                    f.write("WORDS:\n")
                    for word in text_analysis['words']:
                        f.write(f"{word['text']} ")
                    f.write("\n")
            
            # Create a summary file with all unique sentences
            with open(f"{OUTPUT_DIR}/unique_sentences_summary.txt", "a", encoding="utf-8") as f:
                f.write(f"\n[{readable_time}] Frame {frame_id}:\n")
                for sentence in filtered_sentences:
                    f.write(f"  {sentence['text']}\n")
        
        else:
            print("Skipping detailed file output (no new content)")

        # Save frame with boxes (only if we have valid results)
        if result and result[0]:
            debug_frame = draw_boxes(frame.copy(), result[0])
            # Add duplicate status overlay
            if not output_data['has_new_content']:
                cv2.putText(debug_frame, "ALL DUPLICATES", (50, 50), 
                           cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
            else:
                cv2.putText(debug_frame, f"NEW: {len(filtered_sentences)} sentences", (50, 50), 
                           cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}.jpg", debug_frame)
        else:
            cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_noboxes.jpg", frame)

        # Print processing result
        if output_data['has_new_content']:
            print(f"Frame {frame_id} processed with NEW content.\n")
        else:
            print(f"Frame {frame_id} processed - all duplicates.\n")

    except Exception as e:
        print(f"Error processing frame {frame_id}: {e}")
        import traceback
        traceback.print_exc()
        # Save the problematic frame for debugging
        try:
            if 'frame' in locals() and frame is not None:
                cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_error.jpg", frame)
        except:
            pass

def consume_frames():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="ocr-consumer",
        value_deserializer=lambda m: m  # Keep as bytes for now
    )

    print("Listening for frames...")
    print(f"Input Topic: {INPUT_TOPIC}")
    print(f"Output Topic: {OUTPUT_TOPIC}")
    print("=" * 60)
    
    for i, message in enumerate(consumer):
        print(f"Received frame {i}")
        process_frame(message.value, i)

if __name__ == "__main__":
    try:
        consume_frames()
    except KeyboardInterrupt:
        print("\nShutting down...")
        producer.close()
        print("Producer closed successfully")
    except Exception as e:
        print(f"Fatal error: {e}")
        producer.close()