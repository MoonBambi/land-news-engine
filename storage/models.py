# 定义数据表结构（ORM 映射）

class LandNews:
    """
    Example Model for Land News
    """
    def __init__(self, title, content, date, source):
        self.title = title
        self.content = content
        self.date = date
        self.source = source
        # Add more fields as needed (area, location, sentiment, etc.)
