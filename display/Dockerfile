FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Tkinter + GUI + fonts
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-tk libjpeg62-turbo-dev zlib1g-dev libgl1 \
    --fix-missing && rm -rf /var/lib/apt/lists/*


# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY . .

# Run the GUI
CMD ["python", "display.py"]
