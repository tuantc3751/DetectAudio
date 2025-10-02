import pyaudio
import json
import time
import uuid
from confluent_kafka import Producer

KAFKA_BROKER = "10.0.28.44:9092"
TOPIC = f"audio_{uuid.getnode()}"  # topic audio theo MAC address

CHUNK = 1024          # kích thước buffer
FORMAT = pyaudio.paInt16
CHANNELS = 1          # mono
RATE = 16000          # 16kHz
RECORD_SECONDS = 5    # ví dụ thu mỗi lần 5 giây

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Audio delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def record_and_send():
    audio = pyaudio.PyAudio()
    stream = audio.open(format=FORMAT,
                        channels=CHANNELS,
                        rate=RATE,
                        input=True,
                        frames_per_buffer=CHUNK)

    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    print("🎤 Recording...")
    frames = []

    for _ in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
        data = stream.read(CHUNK)
        frames.append(data)

    stream.stop_stream()
    stream.close()
    audio.terminate()

    # gộp bytes lại
    audio_bytes = b''.join(frames)

    # gửi Kafka (nên encode base64 nếu cần JSON)
    producer.produce(
        TOPIC,
        key="mic_audio",
        value=audio_bytes,
        callback=delivery_report
    )
    producer.flush()
    print("✅ Sent audio chunk")

if __name__ == "__main__":
    while True:
        record_and_send()
        time.sleep(1)  # thu xong nghỉ 1s rồi gửi tiếp
