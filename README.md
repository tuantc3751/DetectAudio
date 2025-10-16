# My Project
Dự án này dùng để phát hiện trầm/hưng cảm dựa trên âm thanh.

## 🛠️ Cách cài đặt
```bash
git clone https://github.com/tuantc3751/DetectAudio
cd DetectAudio
pip install -r requirements.txt

# Note
File sysinfo.py dùng để gửi thông tin thiết bị
File sendMinIo.py dùng để gửi âm thanh đến lưu trữ tại MinIO
Trên Iot, cài portaudio cho python nếu chưa có:
- sudo apt install portaudio19-dev python3-pyaudio -y
