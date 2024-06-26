import sqlite3
import pandas as pd 
from typing import Dict, Any
class OnlineStore:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)

    def create_table(self, table_name: str, schema: Dict[str, str]):
        columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

    def insert_data(self, table_name: str, data: pd.DataFrame):
        data.to_sql(table_name, self.conn, if_exists='append', index=False)

    def get_online_features(self, table_name: str, entity_column: str, entity_value: Any) -> Dict[str, Any]:
        query = f"SELECT * FROM {table_name} WHERE {entity_column} = ?"
        cursor = self.conn.execute(query, (entity_value,))
        result = cursor.fetchone()
        if result:
            return dict(zip([column[0] for column in cursor.description], result))
        return None