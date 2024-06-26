import duckdb
import pandas as pd
from typing import Dict, Any, List

class OfflineStore:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)

    def create_table(self, table_name: str, schema: Dict[str, str]):
        columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

    def insert_data(self, table_name: str, data: pd.DataFrame):
        self.conn.register('data_df', data)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM data_df")

    def get_batch_features(self, table_name: str, entity_column: str, entity_values: List[Any]) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name} WHERE {entity_column} IN ({','.join(map(str, entity_values))})"
        return self.conn.execute(query).fetchdf()

    def get_all_entity_ids(self, table_name: str, entity_column: str) -> List[Any]:
        query = f"SELECT DISTINCT {entity_column} FROM {table_name}"
        result = self.conn.execute(query).fetchdf()
        return result[entity_column].tolist()

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()

    def close(self):
        self.conn.close()

    def __del__(self):
        self.close()