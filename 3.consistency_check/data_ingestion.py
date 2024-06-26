import pandas as pd
from typing import Dict, Any

def ingest_data(data: pd.DataFrame, offline_store: Any, online_store: Any, table_name: str):
    # Insert data into offline store
    offline_store.insert_data(table_name, data)
    
    # Insert data into online store
    online_store.insert_data(table_name, data)