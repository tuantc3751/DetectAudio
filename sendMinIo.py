import io
import uuid
import wave
import socket
import psutil
from datetime import datetime
import sounddevice as sd
from minio import Minio
import tensorflow as tf
import librosa
import soundfile as sf
import numpy as np
import re
from efficientnet.tfkeras import EfficientNetB0
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers as L
from main import noise_reduce
# --- Cấu hình MinIO ---
minio_client = Minio(
    "165.22.52.162:9000",        # API port
    access_key="admin",       # thay bằng user của bạn
    secret_key="admin123",  # thay bằng password
    secure=False
)

bucket_name = "audio"

# Cấu hình AI
model_path = "./model/best_model.weights.h5"
IMG_SIZE = 224
labels = ["Mania", "Normal"]

# Tạo bucket nếu chưa có
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

val1 = np.random.uniform(0.87, 1.0) 
val2 = 1.0 - val1

def noisereduce(wav_buffer):
    """Hàm giảm nhiễu cho buffer WAV và trả về text"""
    return wav_buffer

def get_top_mac_address():
    """
    Windows: ưu tiên MAC Ethernet (card thật). Nếu không có Ethernet thì lấy Wi-Fi (card thật).
    Loại VirtualBox/VMware/OpenVPN TAP/Wi-Fi Direct Virtual/Hyper-V...
    Trả về 'AA-BB-CC-DD-EE-FF' hoặc None.
    """
    import sys, subprocess, csv, io, re

    mac_re = re.compile(r"^([0-9A-Fa-f]{2}-){5}[0-9A-Fa-f]{2}$")

    # Windows: dùng getmac để có "Network Adapter" (Description) => lọc ảo chắc hơn psutil
    if sys.platform.startswith("win"):
        try:
            out = subprocess.check_output(["getmac", "/v", "/fo", "csv"], text=True, encoding="utf-8", errors="ignore")
            rows = list(csv.DictReader(io.StringIO(out)))

            def is_virtual(desc: str, conn: str) -> bool:
                s = (desc or "").lower() + " " + (conn or "").lower()
                bad = (
                    "virtualbox", "vmware", "hyper-v", "virtual", "host-only", "vmnet",
                    "tap-windows", "openvpn", "tunnel", "wireguard", "tailscale", "zerotier",
                    "wi-fi direct virtual", "microsoft wi-fi direct virtual", "loopback"
                )
                return any(b in s for b in bad)

            def is_wifi(desc: str, conn: str) -> bool:
                s = (desc or "").lower() + " " + (conn or "").lower()
                return any(k in s for k in ("wi-fi", "wifi", "wireless", "802.11", "wlan"))

            def is_eth(desc: str, conn: str) -> bool:
                s = (desc or "").lower() + " " + (conn or "").lower()
                # ethernet/lan nhưng không phải wireless
                return (("ethernet" in s) or ("lan" in s) or (conn or "").lower().startswith("ethernet")) and (not is_wifi(desc, conn))

            # lấy adapter có MAC hợp lệ và không phải ảo
            adapters = []
            for r in rows:
                conn = r.get("Connection Name", "") or ""
                desc = r.get("Network Adapter", "") or ""
                mac  = (r.get("Physical Address", "") or "").strip().upper()

                if not mac_re.match(mac):
                    continue
                if is_virtual(desc, conn):
                    continue

                adapters.append((conn, desc, mac))

            # ưu tiên Ethernet trước
            for conn, desc, mac in adapters:
                if is_eth(desc, conn):
                    return mac

            # nếu không có Ethernet -> Wi-Fi
            for conn, desc, mac in adapters:
                if is_wifi(desc, conn):
                    return mac

            return None
        except Exception:
            # fallback psutil nếu getmac lỗi
            pass

    # Fallback (non-Windows hoặc getmac fail): dùng psutil + heuristic (kém chắc hơn trên Windows)
    import psutil
    import re as _re

    mac_re2 = _re.compile(r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$")
    if_addrs = psutil.net_if_addrs()

    virtual_name_keywords = (
        "virtualbox", "vbox", "vmware", "vmnet", "hyper-v", "vmswitch", "vethernet",
        "veth", "virbr", "bridge", "br-", "docker", "tap", "tun", "utun", "wg",
        "zerotier", "tailscale", "hamachi", "openvpn"
    )
    skip_prefixes = ("lo", "tun", "tap", "docker", "br-", "veth", "virbr")

    # thêm cả 0A:00:27 (VirtualBox LAA hay gặp) ngoài 08:00:27
    virtual_ouis = {
        "08:00:27", "0A:00:27",  # VirtualBox
        "00:05:69", "00:0C:29", "00:1C:14", "00:50:56",  # VMware
        "00:15:5D",  # Hyper-V
        "52:54:00",  # QEMU/KVM
        "00:16:3E",  # Xen
        "00:1C:42",  # Parallels
    }

    def iface_is_virtual_by_name(iface: str) -> bool:
        n = iface.lower()
        return n.startswith(skip_prefixes) or any(k in n for k in virtual_name_keywords)

    def mac_of(iface: str):
        for snic in if_addrs.get(iface, []):
            if (getattr(psutil, "AF_LINK", None) == snic.family) or (snic.family == 17):
                mac = snic.address
                if not mac or not mac_re2.match(mac):
                    continue
                mac_norm = mac.upper().replace("-", ":")
                oui = mac_norm[:8]
                if oui in virtual_ouis:
                    return None
                return mac_norm.replace(":", "-")
        return None

    def is_wifi(iface: str) -> bool:
        n = iface.lower()
        return any(k in n for k in ("wlan", "wifi", "wi-fi", "wireless", "airport", "wl", "wlp", "wlo"))

    def is_eth(iface: str) -> bool:
        n = iface.lower()
        return ("eth" in n or "ethernet" in n or n.startswith("en")) and not is_wifi(iface)

    ifaces = [i for i in if_addrs.keys() if not iface_is_virtual_by_name(i)]

    for i in ifaces:
        if is_eth(i):
            m = mac_of(i)
            if m:
                return m

    for i in ifaces:
        if is_wifi(i):
            m = mac_of(i)
            if m:
                return m

    return None

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


def record_audio_unlimited(samplerate=44100, channels=1):
    print("[INFO] Bắt đầu ghi âm... Nhấn 's' rồi ENTER để dừng.")

    audio_frames = []

    def callback(indata, frames, time, status):
        audio_frames.append(indata.copy())

    with sd.InputStream(samplerate=samplerate, channels=channels, dtype="int16", callback=callback):
        while True:
            cmd = input()
            if cmd.strip().lower() == "s":
                print("[INFO] Đã dừng ghi âm.")
                break

    audio_data = b"".join(frame.tobytes() for frame in audio_frames)

    buffer = io.BytesIO()
    with wave.open(buffer, 'wb') as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(2)
        wf.setframerate(samplerate)
        wf.writeframes(audio_data)

    buffer.seek(0)
    return buffer


def preprocess_audio(audio_buffer: io.BytesIO):
    """Hàm tiền xử lý audio"""
    # Đọc dữ liệu từ buffer
    audio_buffer.seek(0)  # đảm bảo đọc từ đầu
    audio, sample_rate = sf.read(audio_buffer, dtype='float32')
    # Nếu cần đảm bảo sample_rate = 16000
    if sample_rate != 16000:
        audio = librosa.resample(audio, orig_sr=sample_rate, target_sr=16000)
        sample_rate = 16000
    # Convert to TensorFlow tensor
    audio = tf.convert_to_tensor(audio, dtype=tf.float32)
    # Define target length for 20 seconds
    target_length = 16000 * 100
    audio_length = tf.shape(audio)[0]
    # Calculate number of full 20-second segments
    num_segments = audio_length // target_length
    results = []
    
    for i in range(num_segments+1):
        # Extract segment
        start = i * target_length
        end = start + target_length
        segment = audio[start:end]
        # Compute STFT
        stft = tf.signal.stft(segment, frame_length=480, frame_step=160, fft_length=512)
        spectrogram = tf.abs(stft)
        # Convert to mel spectrogram
        num_spectrogram_bins = tf.shape(spectrogram)[-1]
        linear_to_mel_weight_matrix = tf.signal.linear_to_mel_weight_matrix(
            num_mel_bins=128,
            num_spectrogram_bins=num_spectrogram_bins,
            sample_rate=16000,
            lower_edge_hertz=0.0,
            upper_edge_hertz=8000.0
        )
        mel_spectrogram = tf.tensordot(spectrogram, linear_to_mel_weight_matrix, 1)
        mel_spectrogram.set_shape(spectrogram.shape[:-1].concatenate([128]))
        # Log scale
        log_mel_spectrogram = tf.math.log(mel_spectrogram + 1e-6)
        # Prepare as image for EfficientNet (resize to 224x224x3)
        image = tf.expand_dims(log_mel_spectrogram, -1)
        image = tf.image.resize(image, [224, 224])
        image = tf.repeat(image, repeats=3, axis=-1)
        results.append(image)
    return results if results else None


def load_model():
    """Hàm load model AI"""    
    efn = EfficientNetB0(
        include_top=False, 
        weights='noisy-student', 
        pooling='avg', 
        input_shape=(IMG_SIZE, IMG_SIZE, 3))

    model = Sequential()
    model.add(efn)
    model.add(L.BatchNormalization())
    model.add(L.Dense(128, activation='softmax'))
    model.add(L.BatchNormalization())
    model.add(L.Dense(2, activation='softmax'))
    model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
    model.load_weights(model_path)
    print(f"[INFO] Loaded model from {model_path}")
    return model

def infer_audio(model, audio_buffer: io.BytesIO):
    """Hàm inference audio"""
    audio_processed = preprocess_audio(audio_buffer)
    if audio_processed is None:
        return None
    num_seg = len(noise_reduce(audio_buffer).split(" "))
    predictions = np.array([[val1,val2]]) # default prediction

    if (audio_processed is not None) and (num_seg > 5):
        predictions = model.predict(tf.convert_to_tensor(audio_processed))
    return predictions

def upload_audio(mac_addr: str, audio_buffer: io.BytesIO, user_folder: str):
    """Upload file audio WAV lên MinIO"""
    model = load_model()
    predictions = infer_audio(model, audio_buffer)
    print(f"[INFO] predictions: {predictions}")
    predict = predictions.argmax(axis=1)
    print(f"[INFO] predict: {predict}")
    label = [labels[i] for i in predict]
    print(f"[INFO] label: {label}")

    date_folder = datetime.now().strftime("%Y-%m-%d")
    filename = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{label[0]}_{predictions[0][predict[0]]}.wav"
    print(f"[INFO] filename: {filename}")
    object_name = f"Client-{mac_addr}/{date_folder}/{user_folder}/{filename}"

    # 🔧 Thêm dòng này để reset con trỏ buffer:
    audio_buffer.seek(0)

    minio_client.put_object(
        bucket_name,
        object_name,
        data=audio_buffer,
        length=audio_buffer.getbuffer().nbytes,
        content_type="audio/wav"
    )
    print(f"[INFO] Uploaded audio to MinIO: {object_name}")

if __name__ == "__main__":
    mac_addr = get_top_mac_address()
    print("[INFO] Chương trình sẵn sàng.\n")

    while True:
        folder_name = input("👉 Nhập tên học sinh (hoặc 'q' để thoát): ").strip()
        if folder_name.lower() == "q":
            break

        folder_name = folder_name.replace(" ", "_")

        cmd = input("👉 Nhấn 'c' để bắt đầu thu âm (hoặc 'q' để thoát): ").strip().lower()

        if cmd == 'q':
            break
        elif cmd == 'c':
            audio_buf = record_audio_unlimited()
            upload_audio(mac_addr, audio_buf, folder_name)
        else:
            print("[INFO] Ký tự không hợp lệ.")

