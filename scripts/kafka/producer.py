import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
TOPIC = "user_events"
BOOTSTRAP_SERVERS = "localhost:9092"

# Create producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    linger_ms=5
)

EVENT_TYPES = ["click", "view", "purchase"]


def generate_event():
    """Generate a random user event"""
    return {
        "user_id": random.randint(1, 10000),
        "event": random.choice(EVENT_TYPES),
        "amount": random.randint(100, 10000),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def delivery_report(err, msg):
    """Callback for message delivery"""
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic} partition {msg.partition}")


def main():
    print("🚀 Kafka Producer Started... Press Ctrl+C to stop")

    try:
        while True:
            event = generate_event()

            # Send to Kafka
            future = producer.send(TOPIC, value=event)
            future.add_callback(lambda metadata: print(f"📤 Sent: {event}"))
            future.add_errback(lambda exc: print(f"❌ Error: {exc}"))

            # Flush occasionally for reliability
            producer.flush()

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()