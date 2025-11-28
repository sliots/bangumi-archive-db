import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_batch

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

from config import BATCH_CONFIG

logger = logging.getLogger(__name__)


class BaseDataProcessor(ABC):
    """基础数据处理器抽象类
    
    提供公共的数据库连接、批处理和文件读取功能。
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        """初始化数据处理器
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_config = db_config
        self.connection = None
        self.batch_size = BATCH_CONFIG['batch_size']
        self.commit_interval = BATCH_CONFIG['commit_interval']
        
    def connect_db(self) -> bool:
        """连接到PostgreSQL数据库
        
        Returns:
            bool: 连接是否成功
        """
        try:
            import psycopg2
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")
    
    def count_lines(self, file_path: str) -> int:
        """快速计算文件行数
        
        Args:
            file_path: 文件路径
            
        Returns:
            int: 文件行数
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for line in f if line.strip())
        except Exception as e:
            logger.warning(f"无法计算文件行数 {file_path}: {e}")
            return 0
    
    def read_jsonlines(self, file_path: str, limit: Optional[int] = None, 
                      show_progress: bool = True) -> List[Dict[str, Any]]:
        """读取jsonlines文件
        
        Args:
            file_path: 文件路径
            limit: 限制读取的行数，None表示读取全部
            show_progress: 是否显示进度条
            
        Returns:
            list: 解析后的JSON对象列表
        """
        data = []
        total_lines = self.count_lines(file_path) if HAS_TQDM and show_progress else 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                iterator = enumerate(f, 1)
                if HAS_TQDM and show_progress and total_lines > 0:
                    iterator = tqdm(iterator, total=total_lines, desc=f"读取 {Path(file_path).name}")
                
                for line_num, line in iterator:
                    if limit and line_num > limit:
                        break
                        
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"第{line_num}行JSON解析失败: {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"第{line_num}行处理失败: {e}")
                            continue
                            
            logger.info(f"成功读取 {file_path}，共 {len(data)} 条有效记录")
            return data
            
        except Exception as e:
            logger.error(f"读取文件 {file_path} 失败: {e}")
            return []
    
    @abstractmethod
    def create_tables(self):
        """创建数据库表（子类实现）"""
        pass
    
    @abstractmethod
    def validate_data(self, item: Dict[str, Any]) -> bool:
        """验证数据有效性（子类实现）
        
        Args:
            item: 数据项
            
        Returns:
            bool: 数据是否有效
        """
        pass
    
    @abstractmethod
    def extract_fields(self, item: Dict[str, Any]) -> tuple:
        """提取需要的字段（子类实现）
        
        Args:
            item: 原始数据项
            
        Returns:
            tuple: 提取的字段元组
        """
        pass
    
    @abstractmethod
    def get_upsert_sql(self) -> str:
        """获取UPSERT SQL语句（子类实现）
        
        Returns:
            str: SQL语句
        """
        pass
    

    
    def process_batch(self, batch_data: List[Dict[str, Any]]) -> tuple:
        """批量处理数据
        
        Args:
            batch_data: 批量数据
            
        Returns:
            tuple: (成功数量, 失败数量)
        """
        try:
            with self.connection.cursor() as cursor:
                insert_data = []
                error_count = 0
                
                for item in batch_data:
                    try:
                        if not self.validate_data(item):
                            logger.warning(f"跳过无效数据: {item.get('id', 'unknown')}")
                            error_count += 1
                            continue
                        
                        extracted_fields = self.extract_fields(item)
                        insert_data.append(extracted_fields)
                        
                    except Exception as e:
                        logger.error(f"处理记录失败 {item.get('id', 'unknown')}: {e}")
                        error_count += 1
                        continue
                
                # 批量执行UPSERT
                if insert_data:
                    execute_batch(cursor, self.get_upsert_sql(), insert_data, 
                                page_size=self.batch_size)
                    
                self.connection.commit()
                success_count = len(insert_data)
                logger.info(f"成功处理批次，插入/更新 {success_count} 条记录，失败 {error_count} 条记录")
                
                return success_count, error_count
                
        except Exception as e:
            logger.error(f"批量处理失败: {e}")
            self.connection.rollback()
            raise
    
    def process_file(self, file_path: str, limit: Optional[int] = None):
        """处理文件的通用方法
        
        Args:
            file_path: 文件路径
            limit: 限制处理的记录数
        """
        logger.info(f"开始处理文件: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return
        
        # 读取数据
        data = self.read_jsonlines(file_path, limit)
        if not data:
            logger.warning("没有读取到有效数据")
            return
        
        # 分批处理
        total_batches = (len(data) + self.batch_size - 1) // self.batch_size
        
        if HAS_TQDM:
            progress_bar = tqdm(total=len(data), desc="处理进度")
        
        total_success = 0
        total_error = 0
        
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            
            try:
                success_count, error_count = self.process_batch(batch)
                total_success += success_count
                total_error += error_count
                
                if HAS_TQDM:
                    progress_bar.update(len(batch))
                    
            except Exception as e:
                logger.error(f"处理批次 {i//self.batch_size + 1}/{total_batches} 失败: {e}")
                total_error += len(batch)
                
                if HAS_TQDM:
                    progress_bar.update(len(batch))
        
        if HAS_TQDM:
            progress_bar.close()
        
        logger.info(f"文件处理完成: 成功 {total_success} 条，失败 {total_error} 条")
