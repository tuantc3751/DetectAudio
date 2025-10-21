import io
import uuid
import wave
import socket
import psutil
from datetime import datetime
import sounddevice as sd
from minio import Minio

# --- Cấu hình MinIO ---
minio_client = Minio(
    "160.191.50.208:9000",        # API port
    access_key="admin",       # thay bằng user của bạn
    secret_key="admin123",  # thay bằng password
    secure=False
)

bucket_name = "audio"

# Tạo bucket nếu chưa có
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

def get_top_mac_address():
    net_io = psutil.net_io_counters(pernic=True)
    if_addrs = psutil.net_if_addrs()

    top_iface = None
    top_bytes = 0

    for iface, counters in net_io.items():
        # Bỏ qua loopback và VPN
        if iface.startswith("lo") or iface.startswith("tun") or iface.startswith("docker"):
            continue

        total_bytes = counters.bytes_sent + counters.bytes_recv
        if total_bytes > top_bytes:
            top_bytes = total_bytes
            top_iface = iface

    if not top_iface:
        return None

    mac_addr = None
    for snic in if_addrs[top_iface]:
        # một số hệ thống dùng AF_PACKET thay cho AF_LINK
        if getattr(psutil, "AF_LINK", None) == snic.family or snic.family == 17:
            mac_addr = snic.address
            break

    if mac_addr:
        mac_addr = mac_addr.upper().replace(":", "-")
    return mac_addr

def record_audio(duration=5, samplerate=44100, channels=1):
    """Ghi âm và trả về bytes dạng WAV"""
    print(f"[INFO] Recording {duration}s...")
    audio_data = sd.rec(int(duration * samplerate), samplerate=samplerate,
                        channels=channels, dtype="int16")
    sd.wait()

    # Lưu vào buffer dưới dạng WAV
    buffer = io.BytesIO()
    with wave.open(buffer, 'wb') as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(2)  # int16 -> 2 bytes
        wf.setframerate(samplerate)
        wf.writeframes(audio_data.tobytes())
    buffer.seek(0)
    return buffer


def upload_audio(mac_addr: str, audio_buffer: io.BytesIO):
    """Upload file audio WAV lên MinIO"""
    date_folder = datetime.now().strftime("%Y-%m-%d")
    filename = datetime.now().strftime("%Y%m%d_%H%M%S") + f".wav"
    object_name = f"Client-{mac_addr}/{date_folder}/{filename}"

    minio_client.put_object(
        bucket_name,
        object_name,
        data=audio_buffer,
        length=audio_buffer.getbuffer().nbytes,
        content_type="audio/wav"
    )
    print(f"[INFO] Uploaded audio to MinIO: {object_name}")


if __name__ == "__main__":
    mac_addr= get_top_mac_address()
    while True:
        audio_buf = record_audio(duration=5)
        upload_audio(mac_addr, audio_buf)
