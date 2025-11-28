"""日志配置工具模块

提供统一的日志配置和管理功能。
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from config import LOG_CONFIG


def setup_logger(name: Optional[str] = None, 
                log_file: Optional[str] = None,
                log_level: Optional[str] = None,
                log_format: Optional[str] = None) -> logging.Logger:
    """设置日志记录器
    
    Args:
        name: 日志记录器名称，默认为根记录器
        log_file: 日志文件路径，默认使用配置文件中的设置
        log_level: 日志级别，默认使用配置文件中的设置
        log_format: 日志格式，默认使用配置文件中的设置
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 使用配置文件中的默认值
    log_file = log_file or LOG_CONFIG['file']
    log_level = log_level or LOG_CONFIG['level']
    log_format = log_format or LOG_CONFIG['format']
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    
    # 避免重复配置
    if logger.handlers:
        return logger
    
    # 设置日志级别
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # 创建格式化器
    formatter = logging.Formatter(log_format)
    
    # 创建文件处理器
    if log_file:
        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger
