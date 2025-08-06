from fastapi import FastAPI, HTTPException
from typing import Optional
import boto3
import pandas as pd
import os
from io import BytesIO
import json
from redis_client import r  # Redis client

# MinIO bağlantı bilgileri
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

        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
        body = response["Body"].read()
        dfs.append(pd.read_json(BytesIO(body), lines=True))

    if not dfs:
        raise HTTPException(status_code=404, detail=f"No data files found in '{prefix}'")

    return pd.concat(dfs, ignore_index=True)

# Passenger endpoint (Redis cache entegreli)
@app.get("/metrics/passenger-metrics-5min/{passenger_id}")
def get_passenger_metrics_5min(passenger_id: str,
                               start: Optional[str] = None,
                               end: Optional[str] = None):
    redis_key = f"passenger:{passenger_id}|start:{start}|end:{end}"

    cached = r.get(redis_key)
    if cached:
        print(f">> Redis cache hit: {redis_key}")
        return json.loads(cached)

    df = load_metrics_from_minio("passenger-metrics-5min")
    df = df[df["passenger_id"] == passenger_id]

    if start:
        df = df[df["window_start"] >= start]
    if end:
        df = df[df["window_end"] <= end]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for passenger_id {passenger_id}")

    result = df.to_dict(orient="records")
    print(f">> Writing to Redis: {redis_key}")
    r.set(redis_key, json.dumps(result), ex=300)

    return result

# Driver endpoint (Redis cache entegreli)
@app.get("/metrics/driver-metrics-5min/{driver_id}")
def get_driver_metrics_5min(driver_id: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None):
    redis_key = f"driver:{driver_id}|start:{start}|end:{end}"

    cached = r.get(redis_key)
    if cached:
        print(f">> Redis cache hit: {redis_key}")
        return json.loads(cached)

    df = load_metrics_from_minio("driver-metrics-5min")
    df = df[df["driver_id"] == driver_id]

    if start:
        df = df[df["window_start"] >= start]
    if end:
        df = df[df["window_end"] <= end]

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for driver_id {driver_id}")

    result = df.to_dict(orient="records")
    print(f">> Writing to Redis: {redis_key}")
    r.set(redis_key, json.dumps(result), ex=300)

    return result

# Location endpoint (Redis cache entegreli)
@app.get("/metrics/location-metrics-5min")
def get_location_metrics_5min(start_location_id: Optional[int] = None,
                              end_location_id: Optional[int] = None,
                              start: Optional[str] = None,
                              end: Optional[str] = None):
    redis_key = f"location:startLoc:{start_location_id}|endLoc:{end_location_id}|start:{start}|end:{end}"

    cached = r.get(redis_key)
    if cached:
        print(f">> Redis cache hit: {redis_key}")
        return json.loads(cached)

    df = load_metrics_from_minio("location-metrics-5min")

    if start_location_id is not None:
        df = df[df["start_location_id"] == start_location_id]
    if end_location_id is not None:
        df = df[df["end_location_id"] == end_location_id]
    if start:
        df = df[df["window_start"] >= start]
    if end:
        df = df[df["window_end"] <= end]

    if df.empty:
        raise HTTPException(status_code=404, detail="No location metrics found")

    result = df.to_dict(orient="records")
    print(f">> Writing to Redis: {redis_key}")
    r.set(redis_key, json.dumps(result), ex=300)

    return result

