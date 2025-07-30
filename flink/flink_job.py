import json
import time
import os
from datetime import datetime

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction, SinkFunction
from pyflink.common.serialization import SimpleStringSchema

import boto3


def parse_trip(value: str):
    r = json.loads(value)
    return Types.Row(
        r["trip_id"],
        r["driver_id"],
        r["passenger_id"],
        float(r["duration_min"]),
        float(r["distance_km"]),
        float(r["price"]),
        r.get("vehicle_type", "car"),
        str(r["start_location_id"])
    )


class GenericTripAggregate(AggregateFunction):
    def create_accumulator(self):
        return (0, 0.0, 0.0, 0.0)

    def add(self, value, acc):
        return (
            acc[0] + 1,
            acc[1] + value[3],
            acc[2] + value[4],
            acc[3] + value[5]
        )

    def get_result(self, acc):
        count, total_dur, total_dist, total_price = acc
        avg_dur = total_dur / count if count else 0.0
        return (count, avg_dur, total_dist, total_price)

    def merge(self, acc1, acc2):
        return (
            acc1[0] + acc2[0],
            acc1[1] + acc2[1],
            acc1[2] + acc2[2],
            acc1[3] + acc2[3]
        )


class MinioSink(SinkFunction):
    def __init__(self, prefix: str):
        self.prefix = prefix

    def open(self, runtime_context):
        endpoint = os.getenv("MINIO_ENDPOINT")
        key = os.getenv("MINIO_ACCESS_KEY")
        secret = os.getenv("MINIO_SECRET_KEY")
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            region_name="us-east-1",
            config=boto3.session.Config(signature_version='s3v4')
        )
        # Bucket’ı oluştur (varsa pas geç)
        try:
            self.client.create_bucket(Bucket="metrics")
        except self.client.exceptions.BucketAlreadyOwnedByYou:
            pass

    def invoke(self, value, context):
        # value: (count, avg_dur, total_dist, total_price)
        ts = int(time.time() * 1000)
        key = f"{self.prefix}/part-{ts}.json"
        body = json.dumps({
            "count": value[0],
            "avg_duration": value[1],
            "total_distance": value[2],
            "total_price": value[3]
        })
        self.client.put_object(Bucket="metrics", Key=key, Body=body)


def create_segment_stream(stream, key_selector, prefix):
    return (
        stream
        .key_by(key_selector)
        .window(TumblingProcessingTimeWindows.of(Types.Time.minutes(5)))
        .aggregate(
            GenericTripAggregate(),
            output_type=Types.TUPLE([
                Types.INT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT()
            ])
        )
        .add_sink(MinioSink(prefix))
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer = FlinkKafkaConsumer(
        topics='trip_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flink-group'}
    )
    raw = env.add_source(consumer)

    parsed = raw.map(
        parse_trip,
        output_type=Types.ROW_NAMED(
            ["trip_id","driver_id","passenger_id","duration_min","distance_km","price","vehicle_type","region"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.STRING()]
        )
    )

    # 4 segment:
    create_segment_stream(parsed, lambda r: r.driver_id,    "driver_metrics")
    create_segment_stream(parsed, lambda r: r.passenger_id, "passenger_metrics")
    create_segment_stream(parsed, lambda r: r.region,       "region_metrics")
    create_segment_stream(parsed, lambda r: r.vehicle_type,"vehicle_metrics")

    env.execute("Trip Aggregation to MinIO")


if __name__ == "__main__":
    main()

