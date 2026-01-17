import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "network-traffic"
CSV_PATH = r"C:\Users\Adarsh Pradeep\OneDrive\Desktop\3rd Yr SEM 6\Big Data Analytics\Projects\bigdata_project\36kpi-dataset\40_kpi_output.csv"

KPI_COLUMNS = [
    "Slice_Type","Total_Packets","Throughput_bps","Byte_Velocity",
    "Packet_Efficiency","Avg_IAT","Jitter","IAT_Skewness","IAT_Kurtosis",
    "IAT_PAPR","Min_IAT","Max_IAT","Idle_Rate","Transmission_Duration",
    "Avg_Packet_Size","Pkt_Size_StdDev","Pkt_Size_Skewness","Pkt_Size_Kurtosis",
    "Unique_Pkt_Sizes","Entropy_Score","Small_Pkt_Ratio","Large_Pkt_Ratio",
    "Coeff_Variation_Size","Retransmission_Count","Retransmission_Ratio",
    "Avg_Win_Size","Win_Size_StdDev","Min_Win_Size","Max_Win_Size",
    "Win_Utilization","Zero_Win_Count","UDP_Ratio","Protocol_Diversity",
    "IP_Source_Entropy","Primary_IP_Ratio","Seq_Number_Rate"
]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

df = pd.read_csv(CSV_PATH)

# keep ONLY the 36 KPIs
df = df[KPI_COLUMNS]

print("✅ Streaming 36 KPI columns to Kafka...")

for i, row in df.iterrows():
    producer.send(TOPIC_NAME, value=row.to_dict())

    if i % 1000 == 0:
        print(f"📡 Sent {i} KPI records", end="\r")
        time.sleep(0.05)

producer.flush()
print("\n✅ Streaming complete.")
