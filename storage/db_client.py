# 封装 MySQL/MongoDB 的 CRUD 操作
import pymysql

class MySQLClient:
    def __init__(self, host, user, password, db):
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def insert(self, table, data):
        # TODO: Implement insert logic
        pass

    def query(self, sql):
        # TODO: Implement query logic
        pass
