import psutil
import platform
import socket
import uuid
import time
import json
from datetime import datetime
from confluent_kafka import Producer

import psutil

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

# --- Dùng cùng một MAC ---
MAC_ADDR = get_top_mac_address()
KAFKA_BROKER = "160.191.50.208:9092"
TOPIC = f"Iot-{MAC_ADDR}"

def bytes_to_gb(bytes_value):
    return round(bytes_value / (1024**3), 1)  # làm tròn 1 chữ số thập phân

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
        "total_ram": f"{bytes_to_gb(vm.total)} GB",
        "used_ram": f"{bytes_to_gb(vm.used)} GB",
        "free_ram": f"{bytes_to_gb(vm.available)} GB",
        "ram_usage_percent": vm.percent,
        "total_swap": f"{bytes_to_gb(swap.total)} GB",
        "used_swap": f"{bytes_to_gb(swap.used)} GB",
        "swap_usage_percent": swap.percent,
        "timestamp": datetime.utcnow().isoformat(),
        "allow": True,  # giả định mặc định cho phép
        # "mac_address": ':'.join(['{:02x}'.format((uuid.getnode() >> i) & 0xff)
        #                          for i in range(0, 8*6, 8)][::-1]),
        "mac_address": MAC_ADDR,
        "timespan": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    return info


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    while True:
        sys_info = get_system_info()
        json_data = json.dumps(sys_info)

        producer.produce(
            TOPIC,
            key=sys_info["hostname"],
            value=json_data,
            callback=delivery_report
        )
        producer.flush()
        print(f"Sent system info at {sys_info['timestamp']}")

        time.sleep(10)  # gửi mỗi 10 giây


if __name__ == "__main__":
    main()
