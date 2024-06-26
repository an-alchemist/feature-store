import time
import random
import threading
from queue import Queue
from streaming_processor import StreamingProcessor
from feature_repository import FeatureRepository, FeatureView, Feature
from online_store import OnlineStore
from offline_store import OfflineStore
import pandas as pd

def setup_feature_store():
    feature_repo = FeatureRepository()
    online_store = OnlineStore("online_store.db")
    offline_store = OfflineStore("offline_store.db")

    # Create a sample feature view
    customer_features = FeatureView(
        name="customer_features",
        features=[
            Feature(name="total_purchases", dtype="FLOAT"),
            Feature(name="last_purchase_amount", dtype="FLOAT"),
            Feature(name="last_purchase_time", dtype="FLOAT")
        ],
        entities=["customer_id"],
        ttl=86400,  # 1 day
        version=1
    )
    feature_repo.create_feature_view(customer_features)

    # Create tables in offline and online stores
    schema = {
        "customer_id": "INTEGER",
        "total_purchases": "FLOAT",
        "last_purchase_amount": "FLOAT",
        "last_purchase_time": "FLOAT"
    }
    offline_store.create_table("customer_features", schema)
    online_store.create_table("customer_features", schema)

    return feature_repo, online_store, offline_store

def generate_random_events():
    """Generate a stream of random simulated events."""
    customer_ids = list(range(1, 11))  # 10 customers
    while True:
        yield {
            "customer_id": random.choice(customer_ids),
            "total_purchases": random.uniform(10, 1000),
            "last_purchase_amount": random.uniform(10, 100),
            "last_purchase_time": time.time()
        }
        time.sleep(random.uniform(0.5, 2))  # Random delay between events

def monitor_features(online_store, offline_store):
    """Periodically check and print feature values."""
    while True:
        customer_id = random.randint(1, 10)
        online_features = online_store.get_online_features("customer_features", "customer_id", customer_id)
        offline_features = offline_store.get_batch_features("customer_features", "customer_id", [customer_id])
        
        print(f"\nFeatures for customer {customer_id}:")
        print(f"Online store: {online_features}")
        print(f"Offline store: {offline_features.to_dict('records')[0] if not offline_features.empty else 'No data'}")
        
        time.sleep(1)  # Check every 5 seconds

def process_events(processor, event_generator):
    """Process events from the generator."""
    for event in event_generator:
        print(f"Processing event: {event}")
        processor.process_event(event)
        time.sleep(0.1)  # Small delay to avoid overwhelming the system

def main():
    feature_repo, online_store, offline_store = setup_feature_store()
    processor = StreamingProcessor(feature_repo, online_store, offline_store)

    # Start the monitoring in a separate thread
    monitor_thread = threading.Thread(target=monitor_features, args=(online_store, offline_store))
    monitor_thread.daemon = True
    monitor_thread.start()

    print("Starting streaming processor...")
    try:
        process_events(processor, generate_random_events())
    except KeyboardInterrupt:
        print("Stopping streaming processor...")
    finally:
        print("Cleaning up resources...")
        online_store.close()
        offline_store.close()
        print("Cleanup complete. Exiting.")

if __name__ == "__main__":
    main()