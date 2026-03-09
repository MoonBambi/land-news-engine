# 日志记录器：监控采集任务状态
import logging
import os

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

# Example usage:
# logger = setup_logger('crawler_logger', 'crawler.log')
