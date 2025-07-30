FROM python:3.9-bullseye

WORKDIR /app

# Sistem bağımlılıkları (pyarrow gibi kütüphaneler için gerekli)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    wget \
    git \
    libffi-dev \
    libssl-dev \
    libgl1 \
    netcat \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Sertifikaları güncelle (özellikle kurum ağlarında SSL problemi varsa!)
RUN update-ca-certificates

# Uygulama dosyaları
COPY simulate.py /app/simulate.py
COPY trips.parquet /app/trips.parquet
COPY wait-for-kafka.sh /app/wait-for-kafka.sh

# Python kütüphaneleri (SSL ve timeout için ek parametreler eklendi)
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --timeout 120 \
    pandas kafka-python pyarrow==15.0.2

# Kafka hazır olana kadar beklemek için script
RUN chmod +x /app/wait-for-kafka.sh

# Başlangıç komutu
CMD ["./wait-for-kafka.sh"]
