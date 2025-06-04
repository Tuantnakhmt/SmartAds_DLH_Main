import pandas as pd
import pymysql
import redis
import random
import json

# ---------- Configuration ----------
MYSQL_HOST = "mysql"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "smart_ads"


# ---------- DB Connection ----------
def get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )

# ---------- Table Creation ----------
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    tables = {
        "crm_data": """
            CREATE TABLE IF NOT EXISTS crm_data (
                msisdn VARCHAR(20) PRIMARY KEY,
                age INT,
                gender ENUM('Male', 'Female'),
                address VARCHAR(255)
            )
        """,
        "domain_data": """
            CREATE TABLE IF NOT EXISTS domain_data (
                domain VARCHAR(255) PRIMARY KEY,
                category VARCHAR(100)
            )
        """,
        "ads_config": """
            CREATE TABLE IF NOT EXISTS ads_config (
                store_id INT AUTO_INCREMENT PRIMARY KEY,
                store_name VARCHAR(255),
                address VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT,
                target_domains TEXT,
                target_category VARCHAR(100),
                target_radius FLOAT,
                min_age INT,
                max_age INT
            )
        """,
        "store_target_domains": """
            CREATE TABLE IF NOT EXISTS store_target_domains (
                id INT AUTO_INCREMENT PRIMARY KEY,
                store_id INT,
                domain VARCHAR(255),
                FOREIGN KEY (store_id) REFERENCES ads_config(store_id) ON DELETE CASCADE
            )
        """
    }

    for table_name, create_statement in tables.items():
        cursor.execute(create_statement)
        print(f"✅ Table `{table_name}` checked/created.")

    conn.commit()
    cursor.close()
    conn.close()

# ---------- Insert CRM & Domain ----------
def insert_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_tables()

    crm_chunks = pd.read_csv('/data/crm_data_test.csv', chunksize=10000, dtype={'msisdn': str})
    for chunk in crm_chunks:
        for _, row in chunk.iterrows():
            cursor.execute("INSERT IGNORE INTO crm_data (msisdn, age, gender, address) VALUES (%s, %s, %s, %s)",
                           (row['msisdn'], row['age'], row['gender'], row['address']))

    domain_data = pd.read_csv('/data/domain_data.csv')
    for _, row in domain_data.iterrows():
        cursor.execute("INSERT IGNORE INTO domain_data (domain, category) VALUES (%s, %s)",
                       (row['domain'], row['category']))

    # Store generation
    print("📦 Inserting Store Ads Config...")
    store_names = [
        "Fashion Hub", "Tech World", "Sports Mania", "Game Haven", "News Stand",
        "Style Avenue", "Gamer's Den", "Tech Gurus", "Sports Fanatics", "Daily Digest",
        "Elite Fashion", "Next-Gen Tech", "Fitness Gear", "E-Sports Arena", "Breaking News",
        "Trendy Styles", "Innovators Hub", "Champion's Corner", "Retro Gaming", "World Reports"
    ]
    addresses = [f"{random.randint(100, 999)} Random Street" for _ in range(len(store_names))]
    categories = ["Fashion", "Technology", "Sports", "Gaming", "News"]
    stores = random.sample(list(zip(store_names, addresses)), k=20)

    for store_name, address in stores:
        lat = round(random.uniform(10.0, 20.0), 6)
        lon = round(random.uniform(100.0, 110.0), 6)
        category = random.choice(categories)
        filtered_domains = domain_data[domain_data['category'] == category]['domain']
        if len(filtered_domains) > 0:
            target_domains = ",".join(filtered_domains.sample(n=min(3, len(filtered_domains)), replace=False).tolist())
        else:
            target_domains = ""
        radius = round(random.uniform(1.0, 5.0), 2)
        min_age = random.randint(18, 25)
        max_age = random.randint(35, 60)

        cursor.execute("""
            INSERT INTO ads_config (store_name, address, latitude, longitude, target_domains,
            target_category, target_radius, min_age, max_age)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (store_name, address, lat, lon, target_domains, category, radius, min_age, max_age))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ CRM, domain, and ads_config data inserted.")

# ---------- Insert Store-Domain Mapping ----------
def insert_store_target_domains():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT store_id FROM ads_config ORDER BY store_id ASC LIMIT 5")
    stores = cursor.fetchall()

    domain_data = pd.read_csv('/data/domain_data.csv')
    domains = domain_data['domain'].tolist()
    if len(domains) < 3:
        print("❌ Not enough domains!")
        return

    for store in stores:
        store_id = store['store_id']
        chosen = random.sample(domains, 3)
        for domain in chosen:
            cursor.execute("INSERT INTO store_target_domains (store_id, domain) VALUES (%s, %s)",
                           (store_id, domain))
        print(f"✅ Store {store_id} assigned: {', '.join(chosen)}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ store_target_domains inserted.")

# ---------- Redis Cache ----------
# def cache_to_redis():
#     print("⚡ Caching data to Redis...")
#     r = redis.Redis(host="redis", port=6379, decode_responses=True)
#     conn = get_db_connection()
#     cursor = conn.cursor()

#     cursor.execute("SELECT msisdn, age, gender FROM crm_data")
#     for row in cursor.fetchall():
#         r.hset(f"user:{row['msisdn']}", mapping={"age": row["age"], "gender": row["gender"]})

#     cursor.execute("SELECT domain, category FROM domain_data")
#     for row in cursor.fetchall():
#         r.set(f"domain:{row['domain']}", row["category"])

#     cursor.execute("SELECT * FROM ads_config")
#     for row in cursor.fetchall():
#         store_json = {
#             "store_id": row["store_id"],
#             "store_name": row["store_name"],
#             "address": row["address"],
#             "latitude": row["latitude"],
#             "longitude": row["longitude"],
#             "target_category": row["target_category"],
#             "target_radius": row["target_radius"],
#             "min_age": row["min_age"],
#             "max_age": row["max_age"]
#         }
#         r.set(f"store:{row['store_id']}", json.dumps(store_json))

#     cursor.close()
#     conn.close()
#     print("✅ Redis cache populated.")

# ---------- Entry Point ----------
if __name__ == "__main__":
    insert_data()
    insert_store_target_domains()
    # cache_to_redis()
