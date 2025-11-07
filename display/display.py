from kafka import KafkaConsumer
import threading
import tkinter as tk
import json
from PIL import Image, ImageTk
import io
import base64


# Kafka Consumers
consumer_audio = KafkaConsumer(
    'audio-transcripts',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='display-audio-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

consumer_ocr = KafkaConsumer(
    'ocr-sentences',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='display-ocr-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

consumer_diagram = KafkaConsumer(
    'diagram-detections',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='display-diagram-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


# Helper: check if scrollbar is at bottom
def is_at_bottom(canvas):
    first, last = canvas.yview()
    return last == 1.0


# Consume Audio
def consume_audio(consumer, container, canvas):
    for message in consumer:
        msg = message.value
        text = f"[{msg['speaker']}] {msg['start']:.2f}-{msg['end']:.2f}s: {msg['text']}"
        lbl = tk.Label(
            container, text=text, font=("Arial", 12),
            anchor="w", justify="left", bg="black", fg="white",
            wraplength=480  # ensures text fits inside panel
        )
        lbl.pack(anchor="w", padx=5, pady=2)

        if is_at_bottom(canvas):
            container.update_idletasks()
            canvas.yview_moveto(1.0)


# Consume OCR
def consume_ocr(consumer, container, canvas):
    for message in consumer:
        msg = message.value
        text = (f"[Frame {msg['frame_id']}] OCR: \"{msg['sentence']}\" "
                f"(Conf: {msg['confidence']:.2f}, Words: {msg['word_count']}, "
                f"Line: {msg['line_number']}, Time: {msg['readable_time']})")

        lbl = tk.Label(
            container, text=text, font=("Arial", 12),
            anchor="w", justify="left", bg="black", fg="lightgreen",
            wraplength=480
        )
        lbl.pack(anchor="w", padx=5, pady=2)

        if is_at_bottom(canvas):
            container.update_idletasks()
            canvas.yview_moveto(1.0)


# Consume Diagram
def consume_diagram(consumer, container, canvas):
    for message in consumer:
        msg = message.value
        extracted = msg.get('extracted_text', [])
        if isinstance(extracted, list):
            extracted = " ".join(
                [t['text'] for t in extracted if isinstance(t, dict) and 'text' in t]
            ) if extracted else "(no text)"
        elif not extracted:
            extracted = "(no text)"
            
        # inference = msg.get("inference", "").strip()
        # if not inference:
        #     inference = "(no inference)"

        text = (f"[Frame {msg['frame_id']}] Diagram: {msg['diagram_type']} "
                f"(Conf: {msg['confidence']:.2f}, Area: {msg['area']}, "
                f"Aspect: {msg['aspect_ratio']}, Time: {msg['readable_time']}) "
                f"Text: {extracted}"
                f"Inference: {msg['inference']}")

        lbl = tk.Label(
            container, text=text, font=("Arial", 12),
            anchor="w", justify="left", bg="black", fg="orange",
            wraplength=480
        )
        lbl.pack(anchor="w", padx=5, pady=2)
        
        # inf_lbl = tk.Label(
        #     container, text=f"Inference: {inference}",
        #     font=("Arial", 12, "italic"),
        #     anchor="w", justify="left", bg="black", fg="cyan",
        #     wraplength=480
        # )
        # inf_lbl.pack(anchor="w", padx=5, pady=2)

        # If image is present, decode and display it
        if "diagram_image" in msg:
            try:
                img_bytes = base64.b64decode(msg["diagram_image"])
                img = Image.open(io.BytesIO(img_bytes))
                img.thumbnail((450, 300))  # 👈 keeps image bounded inside section
                tk_img = ImageTk.PhotoImage(img)

                img_label = tk.Label(container, image=tk_img, bg="black")
                img_label.image = tk_img  # keep reference
                img_label.pack(anchor="w", padx=5, pady=5)
            except Exception as e:
                print(f"Failed to render diagram image: {e}")

        if is_at_bottom(canvas):
            container.update_idletasks()
            canvas.yview_moveto(1.0)


# Mousewheel Binding
def bind_mousewheel(canvas):
    def _on_mousewheel(event):
        system = canvas.tk.call("tk", "windowingsystem")
        if system == "aqua":  # macOS
            if event.type == "38":  # <MouseWheel>
                canvas.yview_scroll(-1 * event.delta, "units")
            elif event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")
        else:
            if event.num == 4:
                canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                canvas.yview_scroll(1, "units")
            else:
                canvas.yview_scroll(-1 * int(event.delta / 120), "units")

    canvas.bind("<Enter>", lambda _: (
        canvas.bind_all("<MouseWheel>", _on_mousewheel),
        canvas.bind_all("<Button-4>", _on_mousewheel),
        canvas.bind_all("<Button-5>", _on_mousewheel)
    ))
    canvas.bind("<Leave>", lambda _: (
        canvas.unbind_all("<MouseWheel>"),
        canvas.unbind_all("<Button-4>"),
        canvas.unbind_all("<Button-5>")
    ))


def create_scrollable_frame(parent, width=500, height=700, bg="black"):
    canvas = tk.Canvas(parent, width=width, height=height, bg=bg, highlightthickness=0)
    scroll_frame = tk.Frame(canvas, bg=bg)

    scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"), width=width))
    canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
    canvas.pack(side=tk.LEFT, fill="both", expand=True)

    bind_mousewheel(canvas)
    return scroll_frame, canvas


# GUI Setup
def start_gui():
    root = tk.Tk()
    root.title("Meeting Analysis Display")
    root.geometry("1600x800")

    # Audio Panel
    frame_left = tk.Frame(root)
    frame_left.pack(side=tk.LEFT, padx=10, pady=20, fill="both", expand=True)
    tk.Label(frame_left, text="Audio Transcripts", font=("Arial", 14, "bold")).pack()
    audio_frame, audio_canvas = create_scrollable_frame(frame_left, width=500, height=700, bg="black")

    # OCR Panel
    frame_mid = tk.Frame(root)
    frame_mid.pack(side=tk.LEFT, padx=10, pady=20, fill="both", expand=True)
    tk.Label(frame_mid, text="OCR Sentences", font=("Arial", 14, "bold")).pack()
    ocr_frame, ocr_canvas = create_scrollable_frame(frame_mid, width=500, height=700, bg="black")

    # Diagram Panel
    frame_right = tk.Frame(root)
    frame_right.pack(side=tk.LEFT, padx=10, pady=20, fill="both", expand=True)
    tk.Label(frame_right, text="Diagram Output", font=("Arial", 14, "bold")).pack()
    diagram_frame, diagram_canvas = create_scrollable_frame(frame_right, width=500, height=700, bg="black")

    # Start Threads
    threading.Thread(target=consume_audio, args=(consumer_audio, audio_frame, audio_canvas), daemon=True).start()
    threading.Thread(target=consume_ocr, args=(consumer_ocr, ocr_frame, ocr_canvas), daemon=True).start()
    threading.Thread(target=consume_diagram, args=(consumer_diagram, diagram_frame, diagram_canvas), daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    start_gui()
