from fastapi import FastAPI, HTTPException
from typing import Optional
import boto3
import pandas as pd
import os
from io import BytesIO

# MinIO bağlantı bilgileri (docker-compose env)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "metrics")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# MinIO client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

app = FastAPI(title="Trip Metrics API")

def load_metrics_from_minio(prefix: str) -> pd.DataFrame:
    """MinIO'dan belirtilen prefix altındaki tüm part dosyalarını oku"""
    objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)

    if "Contents" not in objects:
        raise HTTPException(status_code=404, detail=f"No data found under prefix '{prefix}'")

    dfs = []
    for obj in objects["Contents"]:
        key = obj["Key"]
        if key.endswith("/") or not key.startswith(prefix):
            continue
        if not key.startswith(f"{prefix}/part-"):
            continue

        # Dosyayı oku
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
        body = response["Body"].read()
        dfs.append(pd.read_json(BytesIO(body), lines=True))

    if not dfs:
        raise HTTPException(status_code=404, detail=f"No data files found in '{prefix}'")

    return pd.concat(dfs, ignore_index=True)


### ---- Passenger metrics 5-min endpoint ---- ###
@app.get("/metrics/passenger-metrics-5min/{passenger_id}")
def get_passenger_metrics_5min(passenger_id: str,
                               start: Optional[str] = None,
                               end: Optional[str] = None):
    df = load_metrics_from_minio("passenger-metrics-5min")
    df = df[df["passenger_id"] == passenger_id]

    if start:
        df = df[df["window_start"] >= start]
    if end:
        df = df[df["window_end"] <= end]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for passenger_id {passenger_id}")
    
    return df.to_dict(orient="records")


### ---- Driver metrics 5-min endpoint ---- ###
@app.get("/metrics/driver-metrics-5min/{driver_id}")
def get_driver_metrics_5min(driver_id: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None):
    df = load_metrics_from_minio("driver-metrics-5min")
    df = df[df["driver_id"] == driver_id]

    if start:
        df = df[df["window_start"] >= start]
    if end:
        df = df[df["window_end"] <= end]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for driver_id {driver_id}")

    return df.to_dict(orient="records")


### ---- Location metrics 5-min endpoint ---- ###
@app.get("/metrics/location-metrics-5min")
def get_location_metrics_5min(start_location_id: Optional[int] = None,
                              end_location_id: Optional[int] = None,
                              start: Optional[str] = None,
                              end: Optional[str] = None):
    df = load_metrics_from_minio("location-metrics-5min")

