-- Checkpoint ayarları
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Kafka source with WATERMARK for event time processing
CREATE TABLE trips (
  trip_id              STRING,
  driver_id            STRING,
  passenger_id         STRING,
  start_time           TIMESTAMP(3),
  end_time             TIMESTAMP(3),
  start_location_id    INT,
  end_location_id      INT,
  distance_km          DOUBLE,
  duration_min         DOUBLE,
  price                DOUBLE,
  surge_multiplier     DOUBLE,
  -- Event time watermark - 30 saniye gecikmeli veri için tolerans
  WATERMARK FOR start_time AS start_time - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'trip_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Driver metrics with 5-minute windows
CREATE TABLE driver_metrics_5min (
  driver_id      STRING,
  trip_count     BIGINT,
  avg_duration   DOUBLE,
  total_distance DOUBLE,
  total_revenue  DOUBLE,
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://metrics/driver-metrics-5min/',
  'format' = 'json',
  'sink.rolling-policy.rollover-interval' = '60s',
  'sink.rolling-policy.check-interval' = '30s'
);

-- Passenger metrics with 5-minute windows
CREATE TABLE passenger_metrics_5min (
  passenger_id   STRING,
  trip_count     BIGINT,
  avg_duration   DOUBLE,
  total_distance DOUBLE,
  total_spent    DOUBLE,
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://metrics/passenger-metrics-5min/',
  'format' = 'json',
  'sink.rolling-policy.rollover-interval' = '60s',
  'sink.rolling-policy.check-interval' = '30s'
);

-- Location metrics with 5-minute windows
CREATE TABLE location_metrics_5min (
  start_location_id INT,
  end_location_id   INT,
  trip_count        BIGINT,
  avg_distance      DOUBLE,
  total_revenue     DOUBLE,
  window_start      TIMESTAMP(3),
  window_end        TIMESTAMP(3)
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://metrics/location-metrics-5min/',
  'format' = 'json',
  'sink.rolling-policy.rollover-interval' = '60s',
  'sink.rolling-policy.check-interval' = '30s'
);

-- Driver aggregation with 5-minute EVENT TIME windows
INSERT INTO driver_metrics_5min
SELECT
  driver_id,
  COUNT(*) AS trip_count,
  ROUND(AVG(duration_min), 2) AS avg_duration,
  ROUND(SUM(distance_km), 2) AS total_distance,
  ROUND(SUM(price), 2) AS total_revenue,
  TUMBLE_START(start_time, INTERVAL '5' MINUTES) AS window_start,
  TUMBLE_END(start_time, INTERVAL '5' MINUTES) AS window_end
FROM trips
GROUP BY
  driver_id,
  TUMBLE(start_time, INTERVAL '5' MINUTES);

-- Passenger aggregation with 5-minute EVENT TIME windows
INSERT INTO passenger_metrics_5min
SELECT
  passenger_id,
  COUNT(*) AS trip_count,
  ROUND(AVG(duration_min), 2) AS avg_duration,
  ROUND(SUM(distance_km), 2) AS total_distance,
  ROUND(SUM(price), 2) AS total_spent,
  TUMBLE_START(start_time, INTERVAL '5' MINUTES) AS window_start,
  TUMBLE_END(start_time, INTERVAL '5' MINUTES) AS window_end
FROM trips
GROUP BY
  passenger_id,
  TUMBLE(start_time, INTERVAL '5' MINUTES);

-- Location pair aggregation with 5-minute EVENT TIME windows
INSERT INTO location_metrics_5min
SELECT
  start_location_id,
  end_location_id,
  COUNT(*) AS trip_count,
  ROUND(AVG(distance_km), 2) AS avg_distance,
  ROUND(SUM(price), 2) AS total_revenue,
  TUMBLE_START(start_time, INTERVAL '5' MINUTES) AS window_start,
  TUMBLE_END(start_time, INTERVAL '5' MINUTES) AS window_end
FROM trips
GROUP BY
  start_location_id,
  end_location_id,
  TUMBLE(start_time, INTERVAL '5' MINUTES);
