# 数据库连接参数、爬虫延迟、情感分析阈值

# Database Configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'password'
MYSQL_DB = 'land_news'

# Crawler Settings
DOWNLOAD_DELAY = 1.0  # seconds
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# Processor Settings
SENTIMENT_THRESHOLD = 0.5
