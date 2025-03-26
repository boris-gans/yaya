from psycopg2.pool import SimpleConnectionPool
from collections import defaultdict
import pika
import json
import os
from dotenv import load_dotenv
import threading
from typing import Dict, List, Any
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
metric_buffers: Dict[str, List[Dict[str, Any]]] = {
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
    Flush metrics by updating existing event records with accumulated counts.
    Now handles both increments and decrements appropriately.
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
                    # Group metrics by event_id and calculate net change
                    event_changes = {}
                    for metric in metric_buffers[metric_type]:
                        event_id = metric['event_id']
                        # If is_decrement is True, count as -1, otherwise count as +1
                        change = -1 if metric.get('is_decrement', False) else 1
                        event_changes[event_id] = event_changes.get(event_id, 0) + change
                        
                    # Filter out events with zero net change
                    event_changes = {k: v for k, v in event_changes.items() if v != 0}
                    
                    if not event_changes:
                        print(f"No net changes to flush for {metric_type}")
                        metric_buffers[metric_type] = []
                        return
                        
                    # Update each event with its net change
                    for event_id, change in event_changes.items():
                        # First check the current value to avoid negative results if needed
                        cur.execute(f"""
                            SELECT num_{metric_type}s FROM event_data WHERE id = %s;
                        """, (event_id,))
                        result = cur.fetchone()
                        
                        if result:
                            current_value = result[0] or 0
                            # If change is negative and would make the total negative, adjust to 0
                            if change < 0 and current_value + change < 0:
                                change = -current_value  # This will make the new value 0
                        
                            print(cur.mogrify(f"""
                                BEGIN;
                                SELECT num_{metric_type}s FROM event_data WHERE id = %s FOR UPDATE;
                                UPDATE event_data SET num_{metric_type}s = num_{metric_type}s + %s WHERE id = %s;
                                COMMIT;
                            """, (event_id, change, event_id)).decode())

                            cur.execute(f"""
                                BEGIN;
                                SELECT num_{metric_type}s FROM event_data WHERE id = %s FOR UPDATE;
                                UPDATE event_data SET num_{metric_type}s = num_{metric_type}s + %s WHERE id = %s;
                                COMMIT;
                            """, (event_id, change, event_id))
                            
                            print(f"Updated event {event_id} with {change} {metric_type} {'decrements' if change < 0 else 'increments'}")
                        else:
                            print(f"Event {event_id} not found, skipping")
                    
                    conn.commit()
                    print(f"Updated {len(event_changes)} events with {metric_type} metrics")
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
