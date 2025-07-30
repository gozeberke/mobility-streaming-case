#!/bin/sh

echo "🕒 Waiting for Kafka to be ready at kafka:9092..."
while ! nc -z kafka 9092; do
  sleep 1
done

echo "✅ Kafka is available — starting simulator..."
python simulate.py

