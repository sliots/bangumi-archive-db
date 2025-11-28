import logging
from typing import Any, Dict

from .base import BaseDataProcessor
from .character_person import CharacterPersonProcessor
from .subject import SubjectProcessor

logger = logging.getLogger(__name__)


class ProcessorFactory:
    """数据处理器工厂类
    
    负责根据数据类型创建相应的数据处理器实例。
    """
    
    # 支持的数据类型
    SUPPORTED_TYPES = {'character', 'person', 'subject'}
    
    @staticmethod
    def create_processor(data_type: str, db_config: Dict[str, Any]) -> BaseDataProcessor:
        """创建数据处理器
        
        Args:
            data_type: 数据类型 ('character', 'person', 'subject')
            db_config: 数据库配置
            
        Returns:
            BaseDataProcessor: 数据处理器实例
            
        Raises:
            ValueError: 不支持的数据类型时抛出
        """
        data_type = data_type.lower().strip()
        
        if data_type not in ProcessorFactory.SUPPORTED_TYPES:
            raise ValueError(
                f"不支持的数据类型: {data_type}. "
                f"支持的类型: {', '.join(ProcessorFactory.SUPPORTED_TYPES)}"
            )
        
        if data_type in ['character', 'person']:
            logger.debug(f"创建 {data_type} 处理器")
            return CharacterPersonProcessor(db_config, data_type)
        elif data_type == 'subject':
            logger.debug("创建 subject 处理器")
            return SubjectProcessor(db_config)
    
    @staticmethod
    def get_supported_types() -> set:
        """获取支持的数据类型列表
        
        Returns:
            set: 支持的数据类型集合
        """
        return ProcessorFactory.SUPPORTED_TYPES.copy()
    
    @staticmethod
    def is_supported_type(data_type: str) -> bool:
        """检查是否支持指定的数据类型
        
        Args:
            data_type: 数据类型
            
        Returns:
            bool: 是否支持
        """
        return data_type.lower().strip() in ProcessorFactory.SUPPORTED_TYPES
