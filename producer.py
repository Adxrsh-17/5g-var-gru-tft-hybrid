import time
import json
import os
import socket
import dpkt
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_TOPIC = "network-traffic"
KAFKA_SERVER = "localhost:9092"

# PATH SETUP
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
PCAP_ROOT_FOLDER = os.path.join(BASE_DIR, "data", "training_data")

# ⛔ FILES TO SKIP
EXCLUDED_FILES = [
    "naver5g3-10M.pcap", 
    "Youtube_cellular.pcap"
]

def get_slice_type(filename, folderpath):
    filename_lower = filename.lower()
    folder_lower = folderpath.lower()
    
    if "mmtc" in folder_lower or "mmtc" in filename_lower: return "mMTC"
    if filename[0].isdigit(): return "mMTC"
    if "naver" in filename_lower: return "eMBB"
    if "youtube" in filename_lower: return "eMBB"
    if "embb" in filename_lower: return "eMBB"
    if "urllc" in filename_lower: return "URLLC"
    return "Normal"

def start_producer():
    if not os.path.exists(PCAP_ROOT_FOLDER):
        print(f"❌ ERROR: Could not find folder: {PCAP_ROOT_FOLDER}")
        return

    print(f"🚀 Connecting to Kafka at {KAFKA_SERVER}...")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=32768,       
        linger_ms=20,            
        compression_type='lz4', 
        acks='1'
    )

    pcap_files = []
    print(f"📂 Scanning {PCAP_ROOT_FOLDER}...")
    
    for root, dirs, files in os.walk(PCAP_ROOT_FOLDER):
        for file in files:
            if not file.endswith(".pcap") and not file.endswith(".pcapng"): 
                continue
            if file in EXCLUDED_FILES:
                continue

            full_path = os.path.join(root, file)
            pcap_files.append(full_path)

    print(f"✅ Found {len(pcap_files)} valid files.")
    
    total_packets = 0

    for filepath in pcap_files:
        filename = os.path.basename(filepath)
        slice_type = get_slice_type(filename, filepath)
        print(f"📄 Streaming {filename} [{slice_type}]...")
        
        try:
            with open(filepath, 'rb') as f:
                # --- AUTO-DETECT FORMAT (PCAP vs PCAPNG) ---
                try:
                    pcap = dpkt.pcap.Reader(f)
                except ValueError:
                    # If PCAP fails, try PCAPNG (reset file pointer first)
                    f.seek(0)
                    try:
                        pcap = dpkt.pcapng.Reader(f)
                    except:
                        print(f"⚠️ Skipped {filename}: Unknown format (not PCAP/PCAPNG)")
                        continue
                
                for timestamp, buf in pcap:
                    # Parse Ethernet
                    try:
                        eth = dpkt.ethernet.Ethernet(buf)
                    except:
                        continue 
                    
                    if not isinstance(eth.data, dpkt.ip.IP):
                        continue
                    
                    ip = eth.data
                    
                    # Protocol
                    protocol = "Other"
                    is_udp = 0
                    is_tcp = 0
                    tcp_win = 0
                    udp_len = 0
                    
                    if isinstance(ip.data, dpkt.tcp.TCP):
                        protocol = "TCP"
                        is_tcp = 1
                        tcp_win = ip.data.win
                    elif isinstance(ip.data, dpkt.udp.UDP):
                        protocol = "UDP"
                        is_udp = 1
                        udp_len = ip.data.ulen
                        
                    try:
                        src_ip = socket.inet_ntoa(ip.src)
                        dst_ip = socket.inet_ntoa(ip.dst)
                    except:
                        continue

                    data = {
                        "timestamp": float(timestamp),
                        "packet_size": len(buf),
                        "src_ip": src_ip,
                        "dst_ip": dst_ip,
                        "protocol": protocol,
                        "tcp_window": tcp_win,
                        "udp_len": udp_len,
                        "is_udp": is_udp,
                        "is_tcp": is_tcp,
                        "slice_type": slice_type
                    }
                    
                    producer.send(KAFKA_TOPIC, data)
                    total_packets += 1
                    
                    if total_packets % 5000 == 0:
                        print(f"   -> Sent {total_packets} packets...")
                        producer.flush()

        except Exception as e:
            print(f"⚠️ Error reading {filename}: {e}")

    producer.flush()
    print("✅ All packets streamed!")

if __name__ == "__main__":
    start_producer()