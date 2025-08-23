# -*- coding: utf-8 -*-

import logging
import queue
from logging.handlers import QueueHandler

# 全局日志队列
log_queue = queue.Queue()

def setup_logging(log_level_str: str = "INFO"):
    """
    配置全局日志系统。
    所有日志记录都将通过一个队列发送，由工作线程处理并转发到UI。
    """
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    # 获取根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 如果已经有处理器，先移除，避免重复添加
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # 创建一个队列处理器，所有日志都将发送到这个队列
    queue_handler = QueueHandler(log_queue)
    
    # 创建一个格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    queue_handler.setFormatter(formatter)
    
    # 将队列处理器添加到根日志记录器
    root_logger.addHandler(queue_handler)

    # 可以选择性地为特定库设置不同的日志级别
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("patchright").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """
    获取一个以模块命名的日志记录器。
    """
    return logging.getLogger(name)

# 初始设置
setup_logging()
