CREATE TABLE trips_debug (
  passenger_id STRING,
  price DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'trip_topic',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'debug-group-1753797170',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE debug_output (
  passenger_id STRING,
  price DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 's3a://metrics/debug/',
  'format' = 'json',
  'sink.rolling-policy.rollover-interval' = '5s',
  'sink.rolling-policy.check-interval' = '1s'
);

INSERT INTO debug_output
SELECT passenger_id, price FROM trips_debug;
