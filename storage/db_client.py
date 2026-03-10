import pymysql
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


class MySQLClient:
    def __init__(self, host: str, user: str, password: str, db: str, port: int = 3306):
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db,
            port=port,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            return

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
        self.connection.commit()
        return self.connection.affected_rows()

    def query(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return list(rows) if rows else []

    def insert_many(self, table: str, rows: Iterable[Dict[str, Any]], columns: Sequence[str]) -> int:
        cols = list(columns)
        placeholders = ", ".join(["%s"] * len(cols))
        col_sql = ", ".join([f"`{c}`" for c in cols])
        sql = f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders})"

        values: List[Tuple[Any, ...]] = []
        for row in rows:
            values.append(tuple(row.get(c) for c in cols))
        if not values:
            return 0

        with self.connection.cursor() as cursor:
            cursor.executemany(sql, values)
        self.connection.commit()
        return self.connection.affected_rows()
