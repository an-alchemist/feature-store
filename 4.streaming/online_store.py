import sqlite3
import pandas as pd
from typing import Dict, Any, Union
from queue import Queue
from threading import Thread

class OnlineStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.queue = Queue()
        self.worker = Thread(target=self._process_queue)
        self.worker.daemon = True
        self.worker.start()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _process_queue(self):
        conn = self._get_connection()
        while True:
            item = self.queue.get()
            if item is None:
                break
            func, args, kwargs = item
            try:
                func(conn, *args, **kwargs)
            except Exception as e:
                print(f"Error processing queue item: {e}")
            finally:
                self.queue.task_done()
        conn.close()

    def create_table(self, table_name: str, schema: Dict[str, str]):
        def _create_table(conn, table_name, schema):
            columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        self.queue.put((_create_table, (table_name, schema), {}))

    def insert_data(self, table_name: str, data: Union[Dict[str, Any], pd.DataFrame]):
        def _insert_data(conn, table_name, data):
            if isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, pd.DataFrame):
                df = data
            else:
                raise ValueError("Data must be either a dictionary or a pandas DataFrame")
            df.to_sql(table_name, conn, if_exists='append', index=False)
        self.queue.put((_insert_data, (table_name, data), {}))

    def get_online_features(self, table_name: str, entity_column: str, entity_value: Any) -> Dict[str, Any]:
        conn = self._get_connection()
        query = f"SELECT * FROM {table_name} WHERE {entity_column} = ?"
        cursor = conn.execute(query, (entity_value,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return dict(zip([column[0] for column in cursor.description], result))
        return None

    def close(self):
        self.queue.put(None)
        self.worker.join()

    def __del__(self):
        self.close()