# 🚖 Mobility Streaming Case Study

Bu proje, gerçek zamanlı bir mobilite (ride-hailing) platformunun veri akışını, işlenmesini ve metrik üretilmesini simüle eden uçtan uca bir mimariyi içerir.  
Tüm bileşenler **Docker Compose** ile containerize edilmiştir.
!!!! Projeyi çeken herkesin Git LFS kurulu olması lazım:

sudo apt install git-lfs
git lfs install
---

## 🏗 Mimari

Simulator (Kafka Producer)
⬇
Kafka (trip_topic)
⬇
Flink SQL (5 min Event-Time Windows)
⬇
MinIO (S3 compatible storage)
⬇
FastAPI (MinIO'dan metrikleri expose eder)


**Bileşenler:**
- **Kafka:** Ham trip eventlerini alır.
- **Flink SQL:** Passenger, Driver ve Location bazında 5 dakikalık window aggregations çalıştırır.
- **MinIO:** Aggregation sonuçlarını S3 uyumlu storage’a yazar.
- **FastAPI:** MinIO’daki dosyaları okuyup REST endpointleri sağlar.

---

## 🚀 Kurulum

### Gereksinimler
- Docker
- Docker Compose v2

### Çalıştırma
Proje kök klasöründe:
```bash
docker-compose up -d --build
Simulator (Kafka Producer)
Kafka’ya veri göndermek için: docker-compose up -d simulator


📡 API Endpointleri
Passenger metrikleri:GET /metrics/passenger-metrics-5min/{passenger_id}
Örnek :http://localhost:8000/metrics/passenger-metrics-5min/P03415

Driver metrikleri :GET /metrics/driver-metrics-5min/{driver_id}
Örnek:http://localhost:8000/metrics/driver-metrics-5min/D0004

Proje Yapısı
mobility-streaming-case/
 ├── api/                # FastAPI servis kodları
 ├── flink/              # Flink SQL job ve connector jar dosyaları
 ├── simulate/           # Kafka simulator (Producer)
 ├── docker-compose.yml  # Tüm stack için Compose
 ├── README.md           # Bu dosya
 └── .gitignore          # Runtime dosyalarını hariç tutar

🖥 Arayüzler
API Dokümantasyonu: http://localhost:8000/docs

Flink UI: http://localhost:8081

MinIO UI: http://localhost:9001 (user: minioadmin, pass: minioadmin)
