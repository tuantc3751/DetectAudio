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
from efficientnet.tfkeras import EfficientNetB0
from tensorflow.keras.models import Sequential
from tensorflow.keras import layers as L

# --- Cấu hình MinIO ---
minio_client = Minio(
    "160.191.50.208:9000",        # API port
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
    # Define target length for 10 seconds
    target_length = 16000 * 10
    audio_length = tf.shape(audio)[0]
    # Calculate number of full 10-second segments
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
    predictions = model.predict(tf.convert_to_tensor(audio_processed))
    return predictions

def upload_audio(mac_addr: str, audio_buffer: io.BytesIO):
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
    object_name = f"Client-{mac_addr}/{date_folder}/{filename}"

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
    print("[INFO] Chương trình sẵn sàng. Nhấn 'c' để bắt đầu ghi âm hoặc 'q' để thoát.\n")

    while True:
        user_input = input("👉 Nhập 'c' để bắt đầu thu âm (hoặc 'q' để thoát): ").strip().lower()
        if user_input == 'q':
            print("[INFO] Kết thúc chương trình.")
            break
        elif user_input == 'c':
            audio_buf = record_audio(duration=5)
            upload_audio(mac_addr, audio_buf)
        else:
            print("[INFO] Ký tự không hợp lệ, vui lòng nhập lại.")
