from celery import Celery
import pika
import json
import os
from dotenv import load_dotenv
from enum import Enum

# Load environment variables
load_dotenv()

# Initialize Celery
celery = Celery(
    "tasks",
    broker=os.getenv("CELERY_BROKER_URL", "pyamqp://guest@localhost//")
)

class MetricType(Enum):
    CLICK = "click"
    IMPRESSION = "impression"
    SHARE = "share"
    SAVE = "save"

@celery.task(name="tasks.publish_metric")
def publish_metric(event_id: int, metric_type: str):
    """
    Publishes an event metric to RabbitMQ with appropriate routing
    """
    try:
        # Validate metric type
        if metric_type not in [m.value for m in MetricType]:
            raise ValueError(f"Invalid metric type: {metric_type}")

        # Create message
        message = {
            "event_id": event_id,
            "metric_type": metric_type
        }

        # Create connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        channel = connection.channel()

        # Declare exchange
        channel.exchange_declare(
            exchange="event_metrics",
            exchange_type="direct",
            durable=True
        )

        # Declare queue for each metric type
        queue_name = f"metric_{metric_type}"
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            exchange="event_metrics",
            queue=queue_name,
            routing_key=metric_type
        )

        # Publish message
        channel.basic_publish(
            exchange="event_metrics",
            routing_key=metric_type,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

        print(f" [x] Sent {metric_type} metric for event {event_id}")
        connection.close()
        return True

    except Exception as e:
        print(f"Error publishing metric: {e}")
        return False