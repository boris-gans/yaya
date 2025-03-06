from psycopg2.pool import SimpleConnectionPool
from collections import defaultdict
import pika
import json
import os
from dotenv import load_dotenv
import threading
from typing import Dict, List
import time

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "database": os.getenv("POSTGRE_DB"),
    "user": os.getenv("POSTGRE_USER"),
    "password": os.getenv("POSTGRE_PW"),
    "host": os.getenv("POSTGRE_HOST"),
    "port": os.getenv("POSTGRE_WRITE_PORT")
}

# Metric buffers
metric_buffers: Dict[str, List[Dict]] = {
    "click": [],
    "impression": [],
    "share": [],
    "save": []
}

# Thread lock for buffer access
buffer_lock = threading.Lock()

# Database connection pool
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=3,
    **DB_CONFIG
)

def flush_metrics(metric_type: str):
    """
    Flush metrics by updating existing event records with accumulated counts
    """
    with buffer_lock:
        if not metric_buffers[metric_type]:
            return

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                # Group metrics by event_id and count them
                event_counts = {}
                for metric in metric_buffers[metric_type]:
                    event_id = metric['event_id']
                    event_counts[event_id] = event_counts.get(event_id, 0) + 1

                # Update each event with its accumulated count
                for event_id, count in event_counts.items():
                    cur.execute(f"""
                        UPDATE event_data 
                        SET num_{metric_type}s = num_{metric_type}s + %s 
                        WHERE id = %s
                    """, (count, event_id))
                
                conn.commit()
                print(f"Updated {len(event_counts)} events with {metric_type} metrics")
                metric_buffers[metric_type] = []

        except Exception as e:
            print(f"Error flushing metrics: {e}")
            conn.rollback()
        finally:
            db_pool.putconn(conn)

def callback(ch, method, properties, body):
    """
    Process incoming metric messages
    """
    try:
        message = json.loads(body)
        metric_type = message['metric_type']

        with buffer_lock:
            metric_buffers[metric_type].append(message)
            
            # Flush if buffer size exceeds threshold
            if len(metric_buffers[metric_type]) >= 10:
                flush_metrics(metric_type)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    """
    Start the RabbitMQ consumer
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    # Declare exchange
    channel.exchange_declare(
        exchange='event_metrics',
        exchange_type='direct',
        durable=True
    )

    # Declare and bind queues for each metric type
    for metric_type in metric_buffers.keys():
        queue_name = f"metric_{metric_type}"
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(
            exchange='event_metrics',
            queue=queue_name,
            routing_key=metric_type
        )
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback
        )

    print(' [*] Waiting for metrics. To exit press CTRL+C')
    
    # Start periodic flush
    def periodic_flush():
        while True:
            time.sleep(60)  # Flush every minute regardless of buffer size
            for metric_type in metric_buffers.keys():
                flush_metrics(metric_type)

    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        db_pool.closeall()

if __name__ == "__main__":
    start_consumer()
