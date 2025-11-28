#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import re
from pathlib import Path
from typing import Dict, Optional

DATABASE_CONFIG: Dict[str, str] = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': str(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'bangumi'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': 'bangumi_data_processor.log'
}

FILE_PATHS = {
    'character_file': 'bangumiArchive/character.jsonlines',
    'person_file': 'bangumiArchive/person.jsonlines',
    'subject_file': 'bangumiArchive/subject.jsonlines'
}

BATCH_CONFIG = {
    'batch_size': 1000,
    'commit_interval': 100
}

def _get_commit_message_dump_date() -> Optional[str]:
    try:
        repo_dir = Path(__file__).parent / 'bangumiArchive'
        if not repo_dir.exists():
            return None
        msg = subprocess.check_output(
            ['git', '-C', str(repo_dir), 'log', '-1', '--pretty=%B'],
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        m = re.search(r'dump-(\d{4}-\d{2}-\d{2})\.', msg)
        if m:
            return m.group(1)
        return None
    except Exception:
        return None

_commit_msg_date = _get_commit_message_dump_date()

if not _commit_msg_date:
    raise RuntimeError('无法从commit消息解析数据日期')

DATA_CONFIG = {
    'data_date': _commit_msg_date,
    'start_date': os.getenv('DATA_START_DATE')
}
