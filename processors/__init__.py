"""Bangumi数据处理器包

提供用于处理Bangumi数据的各种处理器类。
"""

from .base import BaseDataProcessor
from .character_person import CharacterPersonProcessor
from .subject import SubjectProcessor
from .factory import ProcessorFactory

__all__ = [
    'BaseDataProcessor',
    'CharacterPersonProcessor', 
    'SubjectProcessor',
    'ProcessorFactory'
]
