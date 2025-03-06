from psycopg2.pool import SimpleConnectionPool
from collections import defaultdict
import pika
import json
import os
from dotenv import load_dotenv
import threading
from typing import Dict, List
import time
import signal
import sys

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

print(DB_CONFIG)

# Thread lock for buffer access
buffer_lock = threading.Lock()

# Database connection pool
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=3,
    **DB_CONFIG
)

# Add global flag for shutdown
is_shutting_down = False

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global is_shutting_down
    print("\n🛑 Shutdown signal received. Cleaning up...")
    is_shutting_down = True

def cleanup(channel, connection):
    """Cleanup function to properly close connections"""
    try:
        if channel:
            print("Closing RabbitMQ channel...")
            channel.stop_consuming()
            channel.close()
        if connection:
            print("Closing RabbitMQ connection...")
            connection.close()
        if db_pool:
            print("Closing database connections...")
            db_pool.closeall()
        print("✅ Cleanup completed")
    except Exception as e:
        print(f"Error during cleanup: {e}")
    finally:
        sys.exit(0)

def flush_metrics(metric_type: str):
    """
    Flush metrics by updating existing event records with accumulated counts
    """
    print(f"Attempting to acquire lock for flushing {metric_type} metrics")
    try:
        with buffer_lock:
            print(f"Lock acquired for {metric_type} flush")
            if not metric_buffers[metric_type]:
                print(f"No metrics to flush for {metric_type}")
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
                        print(cur.mogrify(f"""
                            BEGIN;
                            SELECT num_{metric_type}s FROM event_data WHERE id = %s FOR UPDATE;
                            UPDATE event_data SET num_{metric_type}s = num_{metric_type}s + %s WHERE id = %s;
                            COMMIT;
                        """, (event_id, count, event_id)).decode())

                        cur.execute(f"""
                            BEGIN;
                            SELECT num_{metric_type}s FROM event_data WHERE id = %s FOR UPDATE;
                            UPDATE event_data SET num_{metric_type}s = num_{metric_type}s + %s WHERE id = %s;
                            COMMIT;
                        """, (event_id, count, event_id))
                    
                    conn.commit()
                    print(f"Updated {len(event_counts)} events with {metric_type} metrics")
                    metric_buffers[metric_type] = []

            except Exception as e:
                print(f"Error flushing metrics: {e}")
                conn.rollback()
            finally:
                db_pool.putconn(conn)
                print(f"Released DB connection for {metric_type}")
    except Exception as e:
        print(f"Error acquiring lock: {e}")
    finally:
        print(f"Lock released for {metric_type}")

def callback(ch, method, properties, body):
    """
    Process incoming metric messages
    """
    try:
        message = json.loads(body)
        metric_type = message['metric_type']

        print(f"\nAttempting to acquire lock for {metric_type} message")
        with buffer_lock:
            print(f"Lock acquired for {metric_type} message")
            metric_buffers[metric_type].append(message)
            current_size = len(metric_buffers[metric_type])
            print(f"Buffer size of {metric_type}: {current_size}")
            
            should_flush = current_size >= 10
            
        print(f"Lock released for {metric_type} message")
        
        # Flush outside the lock if needed
        if should_flush:
            flush_metrics(metric_type)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    """
    Start the RabbitMQ consumer with graceful shutdown
    """
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    connection = None
    channel = None

    try:
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

        print('✅ Connected to RabbitMQ')
        print('📝 Waiting for metrics. Press CTRL+C to exit')
        
        # Start periodic flush
        def periodic_flush():
            while not is_shutting_down:
                time.sleep(60)  # Flush every minute
                if not is_shutting_down:  # Check again after sleep
                    for metric_type in metric_buffers.keys():
                        flush_metrics(metric_type)

        flush_thread = threading.Thread(target=periodic_flush, daemon=True)
        flush_thread.start()

        # Modified consuming loop to check shutdown flag
        while not is_shutting_down:
            try:
                connection.process_data_events(time_limit=1)  # Process messages with timeout
            except pika.exceptions.AMQPError as e:
                print(f"AMQP Error: {e}")
                break

    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        # Final flush of any remaining metrics
        if metric_buffers:
            print("Performing final flush of metrics...")
            for metric_type in metric_buffers.keys():
                if metric_buffers[metric_type]:
                    flush_metrics(metric_type)
        
        cleanup(channel, connection)

if __name__ == "__main__":
    start_consumer()
