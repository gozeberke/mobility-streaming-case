import pandas as pd
import time
import json
import os
from kafka import KafkaProducer

def main():
    # Parquet dosyasını oku
    df = pd.read_parquet('./trips.parquet')
    df['start_time'] = pd.to_datetime(df['start_time'])
    df = df.sort_values('start_time')

    # Kafka ayarları
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    interval = pd.Timedelta(minutes=5)
    current_time = df['start_time'].min()
    end_time = df['start_time'].max()

    while current_time < end_time:
        batch = df[(df['start_time'] >= current_time) & (df['start_time'] < current_time + interval)]
        print(f"Sending batch for window: {current_time} - {current_time + interval}, count: {len(batch)}")

        for _, row in batch.iterrows():
            record = row.to_dict()

            # Timestamp objelerini string'e dönüştür
            for k, v in record.items():
                if isinstance(v, pd.Timestamp):
                    record[k] = v.isoformat()

            producer.send('trip_topic', record)

        producer.flush()
        time.sleep(0.1)
        current_time += interval

    print("Simülasyon tamamlandı.")

if __name__ == "__main__":
    main()

