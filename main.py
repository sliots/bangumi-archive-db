#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bangumi数据提取和插入工具主程序

使用方法: python main.py <data_type> [limit]
参数: data_type (character/person/subject/all), limit (可选)
"""

import sys
import subprocess
import re
from pathlib import Path
from typing import Optional, List, Tuple

import config as cfg

from config import DATABASE_CONFIG, FILE_PATHS, DATA_CONFIG
from processors import ProcessorFactory
from utils import setup_logger

# 配置日志
logger = setup_logger(__name__)


def parse_arguments() -> tuple:
    """解析命令行参数
    
    Returns:
        tuple: (data_types, limit)
    """
    if len(sys.argv) < 2:
        # 默认处理所有数据类型
        data_type = 'all'
        limit = None
        logger.info("未指定数据类型，默认处理所有数据类型")
    else:
        data_type = sys.argv[1].lower()
        limit = None
        
        # 如果第一个参数是数字，则认为用户想要默认处理所有类型但指定了限制数量
        try:
            limit = int(sys.argv[1])
            data_type = 'all'
            logger.info(f"未指定数据类型，默认处理所有数据类型，限制处理前 {limit} 条记录")
        except ValueError:
            # 第一个参数不是数字，按正常流程处理
            if len(sys.argv) > 2:
                try:
                    limit = int(sys.argv[2])
                    logger.info(f"限制处理前 {limit} 条记录")
                except ValueError:
                    logger.warning("无效的限制参数，将处理全部记录")
    
    # 确定要处理的数据类型
    if data_type == 'all':
        data_types = ['character', 'person', 'subject']
    elif ProcessorFactory.is_supported_type(data_type):
        data_types = [data_type]
    else:
        supported_types = ', '.join(ProcessorFactory.get_supported_types())
        logger.error(f"不支持的数据类型: {data_type}. 支持的类型: {supported_types}")
        sys.exit(1)
    
    return data_types, limit


def get_file_path(data_type: str) -> Optional[Path]:
    """获取数据文件路径
    
    Args:
        data_type: 数据类型
        
    Returns:
        Optional[Path]: 文件路径，如果不存在则返回None
    """
    base_dir = Path(__file__).parent
    file_key = f"{data_type}_file"
    
    if file_key not in FILE_PATHS:
        logger.error(f"配置中未找到 {file_key} 路径")
        return None
    
    file_path = base_dir / FILE_PATHS[file_key]
    
    if not file_path.exists():
        logger.warning(f"{data_type} 文件不存在: {file_path}")
        return None
    
    return file_path


def process_data_type(data_type: str, limit: Optional[int] = None) -> bool:
    """处理指定类型的数据
    
    Args:
        data_type: 数据类型
        limit: 限制处理的记录数
        
    Returns:
        bool: 处理是否成功
    """
    logger.info(f"\n=== 开始处理 {data_type} 数据 ===")
    
    try:
        # 创建处理器
        processor = ProcessorFactory.create_processor(data_type, DATABASE_CONFIG)
        
        # 连接数据库
        if not processor.connect_db():
            logger.error(f"无法连接到数据库，跳过 {data_type} 处理")
            return False
        
        try:
            # 创建表
            processor.create_tables()
            
            # 获取文件路径
            file_path = get_file_path(data_type)
            if not file_path:
                return False
            
            # 处理文件
            processor.process_file(str(file_path), limit)
            
            logger.info(f"{data_type} 数据处理完成！")
            return True
            
        except Exception as e:
            logger.error(f"处理 {data_type} 数据失败: {e}")
            return False
        finally:
            # 关闭连接
            processor.close_connection()
            
    except Exception as e:
        logger.error(f"创建 {data_type} 处理器失败: {e}")
        return False


def _repo_dir() -> Path:
    return Path(__file__).parent / 'bangumiArchive'


def _list_dump_commits() -> List[Tuple[str, str, str]]:
    try:
        out = subprocess.check_output(
            ['git', '-C', str(_repo_dir()), 'log', '--reverse', '--pretty=%H %s', 'master'],
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
    except Exception:
        return []
    commits: List[Tuple[str, str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split(' ', 1)
        h = parts[0]
        s = parts[1] if len(parts) > 1 else ''
        m = re.search(r'dump-(\d{4}-\d{2}-\d{2})\.', s)
        if m:
            commits.append((h, m.group(1), s))
    return commits


def _remove_jsonlines():
    base_dir = Path(__file__).parent
    for key in ('character_file', 'person_file', 'subject_file'):
        p = base_dir / FILE_PATHS[key]
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _checkout_commit(h: str) -> bool:
    try:
        subprocess.run(['git', '-C', str(_repo_dir()), 'checkout', '-f', h], check=True)
        return True
    except Exception:
        return False


def _backup_once(limit: Optional[int]) -> bool:
    types = ['character', 'person', 'subject']
    ok = 0
    for t in types:
        if process_data_type(t, limit):
            ok += 1
    return ok == len(types)


def main() -> int:
    """主函数
    
    Returns:
        int: 程序退出码
    """
    logger.info("=== Bangumi数据提取和插入工具 ===")
    if cfg.DATA_CONFIG.get('start_date'):
        logger.info(f"起始日期: {cfg.DATA_CONFIG['start_date']}")
    else:
        logger.info(f"数据日期: {DATA_CONFIG['data_date']}")
    
    try:
        data_types, limit = parse_arguments()
        if cfg.DATA_CONFIG.get('start_date'):
            commits = _list_dump_commits()
            if not commits:
                logger.error("无法获取提交列表")
                return 1
            start_date = str(cfg.DATA_CONFIG['start_date']).strip()
            start_idx = -1
            for i, (_, d, _) in enumerate(commits):
                if d == start_date:
                    start_idx = i
                    break
            if start_idx == -1:
                logger.error(f"未找到起始日期对应的提交: {start_date}")
                return 1
            for h, d, _ in commits[start_idx:]:
                _remove_jsonlines()
                if not _checkout_commit(h):
                    logger.error(f"切换提交失败: {h}")
                    return 1
                cfg.DATA_CONFIG['data_date'] = d
                if not _backup_once(limit):
                    logger.warning(f"提交 {h} 处理部分失败")
                _remove_jsonlines()
            logger.info("迭代处理完成")
            return 0
        else:
            success_count = 0
            total_count = len(data_types)
            for data_type in data_types:
                if process_data_type(data_type, limit):
                    success_count += 1
            logger.info(f"\n=== 处理完成 ===")
            logger.info(f"成功处理: {success_count}/{total_count} 种数据类型")
            if success_count == total_count:
                logger.info("所有数据处理完成！")
                return 0
            else:
                logger.warning("部分数据处理失败")
                return 1
            
    except KeyboardInterrupt:
        logger.info("用户中断程序执行")
        return 1
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
