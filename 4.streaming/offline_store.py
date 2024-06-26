import duckdb
import pandas as pd
import os
from typing import Dict, Any, List, Union

class OfflineStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)  # Remove the existing file to avoid locking issues
            self.conn = duckdb.connect(self.db_path)
        except Exception as e:
            print(f"Error connecting to DuckDB: {e}")
            self.conn = None

    def create_table(self, table_name: str, schema: Dict[str, str]):
        if self.conn is None:
            print("No connection to DuckDB. Trying to reconnect...")
            self.connect()
            if self.conn is None:
                print("Failed to reconnect. Cannot create table.")
                return

        columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
        try:
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        except Exception as e:
            print(f"Error creating table: {e}")

    def insert_data(self, table_name: str, data: Union[Dict[str, Any], pd.DataFrame]):
        if self.conn is None:
            print("No connection to DuckDB. Trying to reconnect...")
            self.connect()
            if self.conn is None:
                print("Failed to reconnect. Cannot insert data.")
                return

        if isinstance(data, dict):
            df = pd.DataFrame([data])
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError("Data must be either a dictionary or a pandas DataFrame")
        
        try:
            self.conn.register('data_df', df)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM data_df")
        except Exception as e:
            print(f"Error inserting data: {e}")

    def get_batch_features(self, table_name: str, entity_column: str, entity_values: List[Any]) -> pd.DataFrame:
        if self.conn is None:
            print("No connection to DuckDB. Trying to reconnect...")
            self.connect()
            if self.conn is None:
                print("Failed to reconnect. Cannot get batch features.")
                return pd.DataFrame()

        query = f"SELECT * FROM {table_name} WHERE {entity_column} IN ({','.join(map(str, entity_values))})"
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Error getting batch features: {e}")
            return pd.DataFrame()

    def get_all_entity_ids(self, table_name: str, entity_column: str) -> List[Any]:
        if self.conn is None:
            print("No connection to DuckDB. Trying to reconnect...")
            self.connect()
            if self.conn is None:
                print("Failed to reconnect. Cannot get entity IDs.")
                return []

        query = f"SELECT DISTINCT {entity_column} FROM {table_name}"
        try:
            result = self.conn.execute(query).fetchdf()
            return result[entity_column].tolist()
        except Exception as e:
            print(f"Error getting entity IDs: {e}")
            return []

    def execute_query(self, query: str) -> pd.DataFrame:
        if self.conn is None:
            print("No connection to DuckDB. Trying to reconnect...")
            self.connect()
            if self.conn is None:
                print("Failed to reconnect. Cannot execute query.")
                return pd.DataFrame()

        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
        self.conn = None

    def __del__(self):
        self.close()