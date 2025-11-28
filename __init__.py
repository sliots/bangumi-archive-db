"""Bangumi数据处理工具包"""

__version__ = '1.0.0'
__author__ = 'UNKNOWN'
__description__ = 'Bangumi数据提取和插入工具'


from .processors import (
    BaseDataProcessor,
    CharacterPersonProcessor,
    SubjectProcessor,
    ProcessorFactory
)
from .utils import setup_logger
from .config import DATABASE_CONFIG, FILE_PATHS, LOG_CONFIG, BATCH_CONFIG, DATA_CONFIG

__all__ = [
    'BaseDataProcessor', 'CharacterPersonProcessor', 'SubjectProcessor', 'ProcessorFactory',
    'setup_logger',
    'DATABASE_CONFIG', 'FILE_PATHS', 'LOG_CONFIG', 'BATCH_CONFIG', 'DATA_CONFIG',
]
