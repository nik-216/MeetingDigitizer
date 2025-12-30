# 1. Upgrade pip
pip install --upgrade pip

# 2. Install PyTorch (CPU version, change if you have CUDA)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# 3. Install BLIP
pip install transformers
pip install git+https://github.com/salesforce/BLIP.git

# 4. Install YOLO models
pip install ultralytics
