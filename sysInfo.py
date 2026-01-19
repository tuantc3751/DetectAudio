import psutil
import platform
import socket
import uuid
import time
import json
from datetime import datetime
import re
from confluent_kafka import Producer

import psutil

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


# --- Dùng cùng một MAC ---
MAC_ADDR = get_top_mac_address()
KAFKA_BROKER = "165.22.52.162:9092"
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
