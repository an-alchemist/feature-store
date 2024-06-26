# Feature Store with FastAPI Serving Layer

This project implements a basic feature store with a FastAPI serving layer. It includes an online store (SQLite), an offline store (DuckDB), and a FastAPI server for feature retrieval.

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/an-alchemist/feature-store.git
   cd feature-store
   cd 1.foundations
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate 
   ```

3. Install the required dependencies:
   ```
   pip install fastapi uvicorn sqlalchemy duckdb pandas
   ```

## Project Structure

```
feature_store/
├── feature_repository.py
├── online_store.py
├── offline_store.py
├── feature_serving.py
├── main.py
└── README.md
```

## Running the Feature Store

To start the feature store with the FastAPI server:

```
uvicorn main:app --reload
```

This will start the FastAPI server on `http://127.0.0.1:8000`.

## API Endpoints

### Get Online Features

Retrieve online features for a specific entity.

- **URL**: `/get_online_features`
- **Method**: POST
- **Request Body**:
  ```json
  {
    "feature_view": "customer_features",
    "entity_column": "customer_id",
    "entity_value": 1
  }
  ```
- **Response**:
  ```json
  {
    "features": {
      "customer_id": 1,
      "total_purchases": 100.0,
      "loyalty_score": 0.5
    }
  }
  ```

### List Feature View Versions

List all versions of a specific feature view.

- **URL**: `/list_feature_view_versions/{feature_view_name}`
- **Method**: GET
- **Response**:
  ```json
  {
    "feature_view": "customer_features",
    "versions": [1, 2, 3]
  }
  ```

## Usage Examples

### Retrieving Online Features

Using curl:

```bash
curl -X POST "http://127.0.0.1:8000/get_online_features" \
     -H "Content-Type: application/json" \
     -d '{"feature_view": "customer_features", "entity_column": "customer_id", "entity_value": 1}'
```

Using Python requests:

```python
import requests

url = "http://127.0.0.1:8000/get_online_features"
data = {
    "feature_view": "customer_features",
    "entity_column": "customer_id",
    "entity_value": 1
}
response = requests.post(url, json=data)
print(response.json())
```

### Listing Feature View Versions

Using curl:

```bash
curl "http://127.0.0.1:8000/list_feature_view_versions/customer_features"
```

Using Python requests:

```python
import requests

url = "http://127.0.0.1:8000/list_feature_view_versions/customer_features"
response = requests.get(url)
print(response.json())
```

## Populating the Feature Store

Before using the API, you need to populate the feature store with some data. You can do this by modifying the `main.py` file to include some sample data insertion:

```python
# In main.py

def setup_feature_store():
    # ... (existing setup code)

    # Sample data ingestion
    sample_data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "total_purchases": [100.0, 500.0, 250.0],
        "loyalty_score": [0.5, 0.9, 0.7]
    })
    ingest_data(sample_data, offline_store, online_store, "customer_features")

    return FeatureStoreService(feature_repo, online_store)

# ... (rest of the main.py file)
```

This will insert some sample data into your feature store when the application starts.

## Next Steps

- Implement feature versioning
- Add time travel queries
- Implement data quality checks
- Add authentication to the API
- Implement batch feature retrieval
