from datetime import datetime, timedelta
import random
import time
import uuid
from confluent_kafka import Producer
import json

delay = 0.7

requests = [
    {
        "id": "rfc-recommendation-model-prod-id",
        "name": "rfc-recommendation-model-prod",
        "type": "EVENT",
        "data": {
            "event": "PREDICT_FAILED",
            "result": {
                "message": "pricejump endpoint completed"
            }
        },
        "session": {},
        "extra": {
            "headers": {
                "Project-ID": "project-id-1",
                "Host": "svc-rfc-recommendation-v2.gvd-services.svc.cluster.local:5000",
            }
        }
    },
    {
        "id": "rfc-recommendation-model-prod-id",
        "name": "rfc-recommendation-model-prod",
        "type": "EVENT",
        "data": {
            "event": "PREDICT_COMPLETED",
            "result": {
                "message": "pricejump endpoint completed"
            }
        },
        "session": {},
        "extra": {
            "headers": {
                "Host": "svc-rfc-recommendation-v2.gvd-services.svc.cluster.local:5000",
                "Project-ID": "project-id-2",
            }
        }
    },
    {
        "id": "rfc-recommendation-model-prod-id",
        "name": "rfc-recommendation-model-prod",
        "type": "EVENT",
        "data": {
            "event": "PREDICT_COMPLETED",
            "result": {
                "message": "pricejump endpoint completed"
            }
        },
        "session": {},
        "extra": {
            "headers": {
                "Host": "svc-rfc-recommendation-v2.gvd-services.svc.cluster.local:5000",
                "Project-ID": "project-id-3",
            }
        }
    }
]

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
# Kafka topic to produce to
kafka_topic = 'ai-event'
# Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
}

# Create Kafka producer
producer = Producer(producer_config)

if __name__ == '__main__':
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(delay)
            # JSON data to be produced
            random_number = random.randint(0, 2)
            random_number2 = random.randint(0, 10)
            json_data = requests[random_number]
            now = datetime.now()
            session = {
                    "id":  uuid.uuid4().hex,
                    "start": f"{now}",
                    "end": f"{now + timedelta(seconds=random.randint(1, 15))}"
                }
            json_data["session"] = session
            # Randon event status
            status = "PREDICT_COMPLETED"
            if random_number2 > 6:
                status = "PREDICT_FAILED"

            json_data["data"]["event"] = status
            # Convert JSON data to string
            json_str = json.dumps(json_data)

            print("Produce msg: user call API request to AI Model: Hotel Room by feature recommendation")
            print(f"Data index '{random_number}' , Event '{status}'")

            # Produce JSON data to Kafka topic
            producer.produce(kafka_topic, key='your_key', value=json_str)

    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible

        # Flush the producer to ensure that all messages are sent
        producer.flush()

        # Close the producer
        producer.close()