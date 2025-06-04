
# 📊 Hệ thống Smart Ads Data Lakehouse

Dự án xây dựng pipeline dữ liệu thông minh cho hệ thống quảng cáo theo thời gian thực và theo lô (batch) bằng kiến trúc **Data Lakehouse**.

---

## 🚀 Tính năng 

- 📡 Xử lý dữ liệu DPI thời gian thực với Kafka
- 🧠 Kiểm tra điều kiện phát voucher theo:
  - Vị trí (GPS)
  - Hành vi domain
  - Nhân khẩu học (tuổi, giới tính)
- 🔁 Ghi log phát voucher vào MySQL
- ⚡ Xử lý dữ liệu theo lô bằng Apache Spark
- 💾 Ghi dữ liệu theo chuẩn Apache Hudi (hỗ trợ time-travel, incremental query)
- 📅 Lên lịch xử lý với Apache Airflow
- 📊 Trực quan hóa dữ liệu bằng Metabase
- 🐳 Đóng gói toàn bộ hệ thống bằng Docker Compose

---

## 🧱 Cấu trúc thư mục chính

```
DataLH_SmartAds/
├── airflow/               # DAG Airflow
├── data/                  # Dữ liệu đầu vào (CRM, domain, DPI)
├── lakehouse/             # Bảng Hudi (gold, silver)
├── spark/
│   └── jobs/
│       ├── analytics/
│       │   └── top_domains_by_category.py
│       └── benchmark/
│           └── benchmark_incremental_vs_full.py
├── scripts/
│   ├── insert_data.py
│   ├── kafka_producer.py
│   ├── consumer_service.py
│   └── simulate_new_voucher_data.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚙️ Hướng dẫn chạy

1. **Khởi động toàn bộ hệ thống**
```bash
docker-compose up -d --build
```

2. **Tải dữ liệu CRM và domain**
```bash
docker exec data_init python /scripts/insert_data.py
```

3. **Gửi dữ liệu DPI giả lập**
```bash
docker exec producer python /scripts/kafka_producer.py
```

4. **Kích hoạt consumer xử lý voucher**
```bash
docker exec consumer python /scripts/consumer_service.py
```

5. **Chạy xử lý batch top domain**
```bash
docker exec spark_(........) spark-submit /app/analytics/top_domains_by_category.py
```

6. **Benchmark tốc độ đọc incremental**
```bash
docker exec spark_(.........) spark-submit /app/benchmark/benchmark_incremental_vs_full.py
```

---

## 📊 Kết quả đạt được

- 🎯 Phát voucher chính xác theo vị trí và hành vi người dùng
- 📈 Tính toán top domain theo sở thích người dùng
- ⚡ Rút ngắn thời gian xử lý (40 phút còn khoảng 6 phút) nhờ Hudi incremental query
- 📊 Trực quan hóa dashboard theo thời gian thực trên Metabase


