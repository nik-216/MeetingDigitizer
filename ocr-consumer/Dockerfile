FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for OpenCV, PaddleOCR, and ffmpeg
RUN apt-get update && apt-get install -y \
    libgl1 libglib2.0-0 ffmpeg build-essential \
    python3-dev pkg-config \
    libavformat-dev libavcodec-dev libavdevice-dev libavutil-dev libswscale-dev libavfilter-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip/setuptools/wheel before installing requirements
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer code
COPY . .

# Run the OCR consumer
CMD ["python", "ocr_consumer.py"]
