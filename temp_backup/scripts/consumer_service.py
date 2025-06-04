from kafka import KafkaConsumer
import json
import redis
import pymysql
import time
import math
from math import radians, cos, sin, sqrt, atan2

# ------------------ Configuration ------------------
KAFKA_TOPIC = "dpi_data_test"
KAFKA_SERVERS = ["kafka:9092"]

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

conn = pymysql.connect(
    host="mysql",
    user="root",
    password="root",
    database="smart_ads",
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()

# ✅ Create log_voucher table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS log_voucher (
        id INT AUTO_INCREMENT PRIMARY KEY,
        msisdn VARCHAR(20),
        province_name VARCHAR(100),
        province_id VARCHAR(20),
        new_category VARCHAR(100),
        domain VARCHAR(255),
        store_id INT,
        store_name VARCHAR(255),
        gender VARCHAR(10),
        age INT,
        type_ad INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

# ✅ Preload store configs
start_time = time.time()
cursor.execute("SELECT store_id, store_name, latitude, longitude, target_radius, min_age, max_age, target_category FROM ads_config")
stores = cursor.fetchall()
print(f"⏳ Store preload: {time.time() - start_time:.4f} sec")

# ✅ Cache stores to Redis
for store in stores:
    redis_client.set(f"store:{store['target_radius']}:{store['target_category']}", json.dumps(store))

# ✅ Preload store-domain mappings
cursor.execute("SELECT store_id, domain FROM store_target_domains")
store_domain_mappings = {}
for row in cursor.fetchall():
    if row["store_id"] not in store_domain_mappings:
        store_domain_mappings[row["store_id"]] = set()
    if row["domain"]:
        store_domain_mappings[row["store_id"]].add(row["domain"])
print("✅ Cached store-targeted domains")

# # ✅ Haversine Distance Function
# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371
#     lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
#     dlat, dlon = lat2 - lat1, lon2 - lon1
#     a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
#     return R * 2 * atan2(sqrt(a), sqrt(1-a))

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth's radius in km
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return (R * c) / 100  # Distance in km

# ✅ Get user info (Redis first, fallback to MySQL)
def get_user_info(msisdn):
    redis_start = time.time()
    cached = redis_client.get(f"user:{msisdn}")
    if cached:
        print(f"✅ Redis fetch time: {time.time() - redis_start:.6f}s")
        return json.loads(cached)

    mysql_start = time.time()
    cursor.execute("SELECT age, gender FROM crm_data WHERE msisdn = %s", (msisdn,))
    row = cursor.fetchone()
    print(f"⏳ MySQL fetch time: {time.time() - mysql_start:.6f}s")
    if row:
        redis_client.setex(f"user:{msisdn}", 3600, json.dumps(row))
    return row

# ✅ Distribute voucher
def distribute_voucher(data, store, user_info, type_ad):
    print(f"🎉 Voucher granted from {store['store_name']}!")

    def safe(val):
        return None if val in ("", None) or (isinstance(val, float) and math.isnan(val)) else val

    cursor.execute("""
        INSERT INTO log_voucher (msisdn, province_name, province_id, new_category, domain, store_id, store_name, gender, age, type_ad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        safe(data.get("msisdn")),
        safe(data.get("province_name")),
        safe(data.get("province_id")),
        safe(data.get("new_category")),
        safe(data.get("domain")),
        safe(store["store_id"]),
        safe(store["store_name"]),
        safe(user_info.get("gender")),
        safe(user_info.get("age")),
        safe(type_ad)
    ))
    conn.commit()

# ✅ Main eligibility check
def check_user_eligibility(data):
    user_info = get_user_info(data.get("msisdn"))
    if not user_info:
        return

    age_raw = user_info.get("age")
    if age_raw is None:
        print(f"❌ User {data.get('msisdn')} has no age info")
        return

    try:
        age = int(age_raw)
    except ValueError:
        print(f"❌ Invalid age format: {age_raw}")
        return
        
    gender = user_info["gender"]
    lat = float(data["latitude"])
    lon = float(data["longitude"])
    domain = data.get("domain", "")
    category = data.get("new_category")


    for store in stores:
        key = f"store:{store['target_radius']}:{store['target_category']}"
        cached = redis_client.get(key)
        store_data = json.loads(cached) if cached else store  # ← SAFE assignment

        distance = haversine(lat, lon, store_data["latitude"], store_data["longitude"])

        print(f"📍 Store: {store_data['store_name']} - Dist: {distance:.2f} km - Age: {age}")
        print(f"▶ Match Distance: {distance <= float(store_data['target_radius'])}")
        print(f"▶ Match Age: {store_data['min_age']} <= {age} <= {store_data['max_age']}")
        print(f"▶ Match Domain: {domain in store_domain_mappings.get(store_data['store_id'], set())}")
        print(f"▶ Match Category: {category} == {store_data['target_category']}")

        if distance <= float(store_data["target_radius"]) and store_data["min_age"] <= age <= store_data["max_age"]:
            distribute_voucher(data, store_data, user_info, 1)
            return

        if store_data["store_id"] in store_domain_mappings and domain in store_domain_mappings[store_data["store_id"]]:
            distribute_voucher(data, store_data, user_info, 3)
            return

        if category == store_data["target_category"] and store_data["min_age"] <= age <= store_data["max_age"]:
            distribute_voucher(data, store_data, user_info, 2)
            return

# ✅ Start Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("🎯 Kafka Consumer listening...")

for msg in consumer:
    check_user_eligibility(msg.value)
    
