import json
from kafka import KafkaConsumer


def json_deserializer(data):
    return json.loads(data.decode("utf-8"))


consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,  # stops when no more messages
    value_deserializer=json_deserializer,
)

count = 0

for message in consumer:
    trip = message.value

    dist = trip.get("trip_distance")

    if dist is not None and dist > 5:
        count += 1

consumer.close()

print("Trips > 5km =", count)