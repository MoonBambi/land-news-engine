# 管道（数据在存入 DB 前的预处理）

class LandNewsPipeline:
    def process_item(self, item, spider):
        # TODO: Clean and validate item
        return item

class DatabasePipeline:
    def process_item(self, item, spider):
        # TODO: Save item to database
        return item
