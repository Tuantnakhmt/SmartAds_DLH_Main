from kafka import KafkaProducer
import csv
import json
import time

TOPIC = "dpi_data_test"
KAFKA_SERVERS = ["kafka:9092"]
CSV_PATH = "/data/dpi_data_updated.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(CSV_PATH, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        if i >= 5:  # only send 5 rows
            break
        producer.send(TOPIC, value=row)
        print(f"📤 Sent row {i + 1}: {row}")
        time.sleep(10)  # 10 seconds delay

producer.flush()
print("✅ Test complete: 5 rows sent with 10s delay.")
