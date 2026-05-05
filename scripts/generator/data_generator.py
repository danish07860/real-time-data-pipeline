import json
import random
import time
import os
from datetime import datetime

# File path (WSL path)
FILE_PATH = "/mnt/d/real_time_data_pipeline/data/raw/events.json"

# Ensure directory exists
os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

EVENT_TYPES = ["click", "view", "purchase"]


def generate_event():
    return {
        "user_id": random.randint(1, 100),
        "event": random.choice(EVENT_TYPES),
        "amount": random.randint(100, 1000),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def write_event(event):
    try:
        with open(FILE_PATH, "a") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        print(f"Error writing event: {e}")


def main():
    print("Starting data generator... Press Ctrl+C to stop")

    while True:
        event = generate_event()
        write_event(event)

        print(f"Generated: {event}")
        time.sleep(1)


if __name__ == "__main__":
    main()
