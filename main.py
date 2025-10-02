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

KAFKA_BROKER = "10.0.28.44:9092"
TOPIC_SYS = f"client_{uuid.getnode()}"   # topic system info
TOPIC_AUDIO = f"audio_{uuid.getnode()}"  # topic audio

stop_event = threading.Event()  # flag để dừng thread

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
