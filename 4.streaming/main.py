from feature_repository import FeatureRepository, FeatureView, Feature
from offline_store import OfflineStore
from online_store import OnlineStore
from data_ingestion import ingest_data
from feature_serving import app, FeatureStoreService
import pandas as pd

def setup_feature_store():
    # Initialize components
    feature_repo = FeatureRepository()
    offline_store = OfflineStore("offline_store.db")
    online_store = OnlineStore("online_store.db")

    # Create a sample feature view
    customer_features = FeatureView(
        name="customer_features",
        features=[
            Feature(name="age", dtype="INTEGER"),
            Feature(name="total_purchases", dtype="FLOAT"),
            Feature(name="loyalty_score", dtype="FLOAT"),
        ],
        entities=["customer_id"],
        ttl=86400,  # 1 day
        version=1
    )
    feature_repo.create_feature_view(customer_features)

    # Create tables in offline and online stores
    schema = {
        "customer_id": "INTEGER PRIMARY KEY",
        "age": "INTEGER",
        "total_purchases": "FLOAT",
        "loyalty_score": "FLOAT"
    }
    offline_store.create_table("customer_features", schema)
    online_store.create_table("customer_features", schema)

    # Sample data ingestion
    sample_data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "age": [25, 40, 35],
        "total_purchases": [100.0, 500.0, 250.0],
        "loyalty_score": [0.5, 0.9, 0.7]
    })
    ingest_data(sample_data, offline_store, online_store, "customer_features")

    return FeatureStoreService(feature_repo, online_store)

def main():
    # Set up the feature store
    feature_store_service = setup_feature_store()
    
    # Add the feature store service to the app's state
    app.state.feature_store_service = feature_store_service

    # Run the API server
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()