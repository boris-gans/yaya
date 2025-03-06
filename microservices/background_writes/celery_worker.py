from celery import Celery
import pika
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Celery
celery = Celery(
    "tasks",
    broker=os.getenv("CELERY_BROKER_URL", "pyamqp://guest@localhost//")
)

@celery.task(name="tasks.publish_message")
def publish_message(message: str, routing_key: str = "default"):
    """
    Publishes a message to RabbitMQ exchange with specified routing key
    """
    try:
        # Create connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        channel = connection.channel()

        # Declare exchange
        channel.exchange_declare(
            exchange="yaya_events",
            exchange_type="direct",
            durable=True
        )

        # Declare queue and bind it to exchange
        channel.queue_declare(queue="default_queue", durable=True)
        channel.queue_bind(
            exchange="yaya_events",
            queue="default_queue",
            routing_key=routing_key
        )

        # Publish message
        channel.basic_publish(
            exchange="yaya_events",
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

        print(f" [x] Sent {message} with routing key {routing_key}")
        connection.close()
        return True

    except Exception as e:
        print(f"Error publishing message: {e}")
        return False