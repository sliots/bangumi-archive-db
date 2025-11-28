import json
import logging
from typing import Any, Dict

from .base import BaseDataProcessor
from config import DATA_CONFIG

logger = logging.getLogger(__name__)


class SubjectProcessor(BaseDataProcessor):
    """Subject数据处理器
    
    用于处理作品数据的提取、验证和数据库操作。
    """
    
    def create_tables(self):
        """创建subject表"""
        try:
            with self.connection.cursor() as cursor:
                # 创建subject_stats表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS subject_stats (
                        id INTEGER NOT NULL,
                        score DECIMAL(3,1),
                        score_details JSONB,
                        rank INTEGER,
                        favorite JSONB,
                        data_date DATE NOT NULL,
                        PRIMARY KEY (id, data_date)
                    )
                """)
                
                # 创建索引
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subject_stats_score 
                    ON subject_stats(score DESC)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subject_stats_rank 
                    ON subject_stats(rank ASC)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subject_stats_score_details 
                    ON subject_stats USING GIN(score_details)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_subject_stats_favorite 
                    ON subject_stats USING GIN(favorite)
                """)
                
                self.connection.commit()  # type: ignore
                logger.info("Subject表创建成功")
                
        except Exception as e:
            logger.error(f"创建Subject表失败: {e}")
            self.connection.rollback()  # type: ignore
            raise
    
    def validate_data(self, item: Dict[str, Any]) -> bool:
        """验证subject数据
        
        Args:
            item: 数据项
            
        Returns:
            bool: 数据是否有效
        """
        # 检查必需字段
        if 'id' not in item or item['id'] is None:
            return False
            
        # 验证score_details格式
        score_details = item.get('score_details')
        if score_details is not None:
            if not isinstance(score_details, dict):
                return False
            # 检查是否包含1-10的评分键
            for key in score_details.keys():
                if not (key.isdigit() and 1 <= int(key) <= 10):
                    logger.warning(f"无效的score_details键: {key}")
                    
        # 验证favorite格式
        favorite = item.get('favorite')
        if favorite is not None:
            if not isinstance(favorite, dict):
                return False
            # 检查favorite的标准字段
            expected_keys = {'wish', 'done', 'doing', 'on_hold', 'dropped'}
            if not set(favorite.keys()).issubset(expected_keys):
                logger.warning(f"favorite包含未知字段: {set(favorite.keys()) - expected_keys}")
                
        return True
    
    def extract_fields(self, item: Dict[str, Any]) -> tuple:
        """提取subject字段
        
        Args:
            item: 原始数据项
            
        Returns:
            tuple: 提取的字段元组
        """
        # 提取字段
        item_id = item.get('id')
        score = item.get('score')
        score_details = item.get('score_details')
        rank = item.get('rank')
        favorite = item.get('favorite')
        
        # 转换为JSON字符串
        score_details_json = json.dumps(score_details) if score_details is not None else None
        favorite_json = json.dumps(favorite) if favorite is not None else None
        
        return (
            item_id, score, score_details_json, 
            rank, favorite_json, DATA_CONFIG['data_date']
        )
    
    def get_upsert_sql(self) -> str:
        """获取subject的UPSERT SQL
        
        Returns:
            str: UPSERT SQL语句
        """
        return """
            INSERT INTO subject_stats (id, score, score_details, rank, favorite, data_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, data_date) DO UPDATE SET
                score = EXCLUDED.score,
                score_details = EXCLUDED.score_details,
                rank = EXCLUDED.rank,
                favorite = EXCLUDED.favorite
        """
