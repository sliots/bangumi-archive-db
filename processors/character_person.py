import logging
from typing import Any, Dict

from .base import BaseDataProcessor
from config import DATA_CONFIG

logger = logging.getLogger(__name__)


class CharacterPersonProcessor(BaseDataProcessor):
    """Character和Person数据处理器
    
    用于处理角色和人物数据的提取、验证和数据库操作。
    """
    
    def __init__(self, db_config: Dict[str, Any], data_type: str):
        """初始化处理器
        
        Args:
            db_config: 数据库连接配置
            data_type: 数据类型，'character' 或 'person'
        """
        super().__init__(db_config)
        self.data_type = data_type
        self.table_name = f"{data_type}_stats"
    
    def create_tables(self):
        """创建character和person表"""
        try:
            with self.connection.cursor() as cursor:
                # 创建表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id INTEGER NOT NULL,
                        comments INTEGER DEFAULT 0,
                        collects INTEGER DEFAULT 0,
                        data_date DATE NOT NULL,
                        PRIMARY KEY (id, data_date)
                    )
                """)
                
                # 创建索引
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.table_name}_comments 
                    ON {self.table_name}(comments DESC)
                """)
                
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.table_name}_collects 
                    ON {self.table_name}(collects DESC)
                """)
                
                self.connection.commit()  # type: ignore
                logger.info(f"{self.data_type}表创建成功")
                
        except Exception as e:
            logger.error(f"创建{self.data_type}表失败: {e}")
            self.connection.rollback()  # type: ignore
            raise
    
    def validate_data(self, item: Dict[str, Any]) -> bool:
        """验证character/person数据
        
        Args:
            item: 数据项
            
        Returns:
            bool: 数据是否有效
        """
        return 'id' in item and item['id'] is not None
    
    def extract_fields(self, item: Dict[str, Any]) -> tuple:
        """提取character/person字段
        
        Args:
            item: 原始数据项
            
        Returns:
            tuple: 提取的字段元组
        """
        return (
            item['id'],
            int(item.get('comments') or 0),
            int(item.get('collects') or 0),
            DATA_CONFIG['data_date']
        )
    
    def get_upsert_sql(self) -> str:
        """获取character/person的UPSERT SQL
        
        Returns:
            str: UPSERT SQL语句
        """
        return f"""
            INSERT INTO {self.table_name} (id, comments, collects, data_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id, data_date) DO UPDATE SET
                comments = EXCLUDED.comments,
                collects = EXCLUDED.collects
        """
