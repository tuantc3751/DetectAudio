import wave
import psutil
import platform
import socket
import uuid
import time
import json
import threading
import signal
import sys
from datetime import datetime
from confluent_kafka import Producer
import pyaudio
import speech_recognition as sr
import numpy as np
import contextlib
import os

KAFKA_BROKER = "10.0.28.44:9092"
TOPIC_SYS = f"client_{uuid.getnode()}"   # topic system info
TOPIC_AUDIO = f"audio_{uuid.getnode()}"  # topic audio

stop_event = threading.Event()  # flag để dừng thread

def check_volume(wav_buffer):
    wav_buffer.seek(0)
    with wave.open(wav_buffer, 'rb') as wf:
        n_channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        n_frames = wf.getnframes()
        raw_data = wf.readframes(n_frames)
    audio_data = np.frombuffer(raw_data, dtype=np.int16)
    
    if len(audio_data) == 0:
        return 0, 0
    peak_amplitude = np.max(np.abs(audio_data))
    rms_amplitude = np.sqrt(np.mean(audio_data.astype(np.float64)**2))
    return peak_amplitude, rms_amplitude


def noise_reduce(wav_buffer):
    try:
        seconds = get_audio_duration(wav_buffer)
        if seconds < 5.0:
            return "Không cần lọc nhiễu cho wav buffer này nữa"
        peak_amplitude, rms_amplitude = check_volume(wav_buffer)
        if(rms_amplitude < 250):
            return "Không cần lọc nhiễu cho wav buffer này nữa"
        print(f"[INFO] Peak Amplitude: {peak_amplitude}, RMS Amplitude: {rms_amplitude}")
        wav_buffer.seek(0)
        with sr.AudioFile(wav_buffer) as source:
            red.adjust_for_ambient_noise(source, duration=0.5)
            audio_data = red.record(source)
            try:
                text = red.recognize_google(audio_data, language='vi-VN')
                print(f"[INFO] Giảm nhiễu thành công: {text}")
                text
                if any(wav in text for wav in wav_test):
                    return "Không thể reduce noise"
                return text
            except sr.UnknownValueError:
                try:
                    text = red.recognize_google(audio_data, language='en-US')
                    return text
                except sr.UnknownValueError:
                    return "Không thể reduce noise"
    except sr.RequestError as e:
        return f"Lỗi khi xử lý nhiễu cho dữ liệu: {e}"
    except Exception as e:
        return f"Lỗi khi xử lý nhiễu cho dữ liệu: {e}"

def get_audio_duration(wav_buffer):
    """Trả về độ dài âm thanh tính bằng giây (float)"""
    wav_buffer.seek(0)
    
    with contextlib.closing(wave.open(wav_buffer, 'rb')) as wf:
        frames = wf.getnframes()
        rate = wf.getframerate()
        duration = frames / float(rate)
        
    return duration

# -------- System Info --------
def get_system_info():
    uname = platform.uname()
    vm = psutil.virtual_memory()
    swap = psutil.swap_memory()
    cpu_freq = psutil.cpu_freq()

    info = {
        "hostname": socket.gethostname(),
        "platform": uname.system,
        "os": uname.system,
        "os_version": uname.version,
        "uptime": time.strftime("%dd %Hh %Mm", time.gmtime(time.time() - psutil.boot_time())),
        "cpu_model": uname.processor,
        "cpu_cores": psutil.cpu_count(logical=False),
        "cpu_usage_percent": psutil.cpu_percent(interval=1),
        "cpu_frequency_mhz": cpu_freq.current if cpu_freq else 0,
        "total_ram": vm.total,
        "used_ram": vm.used,
        "free_ram": vm.available,
        "ram_usage_percent": vm.percent,
        "total_swap": swap.total,
        "used_swap": swap.used,
        "swap_usage_percent": swap.percent,
        "timestamp": datetime.utcnow().isoformat(),
        "allow": True,
        "mac_address": ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff)
                                for i in range(0, 8*6, 8)][::-1]),
        "timespan": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return info

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# -------- Audio --------
CHUNK = 1024
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 16000
RECORD_SECONDS = 5
red = sr.Recognizer()

def audio_thread(producer):
    audio = pyaudio.PyAudio()
    while not stop_event.is_set():
        stream = audio.open(format=FORMAT,
                            channels=CHANNELS,
                            rate=RATE,
                            input=True,
                            frames_per_buffer=CHUNK)
        print("🎤 Recording...")
        frames = []
        for _ in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
            if stop_event.is_set():
                break
            data = stream.read(CHUNK)
            frames.append(data)
        stream.stop_stream()
        stream.close()

        if frames:
            audio_bytes = b''.join(frames)
            producer.produce(
                TOPIC_AUDIO,
                key="mic_audio",
                value=audio_bytes,
                callback=delivery_report
            )
            producer.flush()
            print("✅ Sent audio chunk")

        time.sleep(1)

    audio.terminate()
    print("🛑 Audio thread stopped.")

# -------- System Info Thread --------
def sysinfo_thread(producer):
    while not stop_event.is_set():
        sys_info = get_system_info()
        json_data = json.dumps(sys_info)

        producer.produce(
            TOPIC_SYS,
            key=sys_info["hostname"],
            value=json_data,
            callback=delivery_report
        )
        producer.flush()
        print(f"📡 Sent system info at {sys_info['timestamp']}")
        time.sleep(10)

    print("🛑 System info thread stopped.")
def load_wavwords(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip().lower() for line in f if line.strip()]

wav_test = load_wavwords("test.wav")

# -------- Main --------
def main():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    t1 = threading.Thread(target=sysinfo_thread, args=(producer,), daemon=True)
    t2 = threading.Thread(target=audio_thread, args=(producer,), daemon=True)

    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⚡ Ctrl+C detected! Stopping...")
        stop_event.set()
        t1.join()
        t2.join()
        producer.flush()
        print("✅ Program exited cleanly.")
        sys.exit(0)

if __name__ == "__main__":
    main()
