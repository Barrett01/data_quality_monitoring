#!/usr/bin/env python
"""服务启动脚本。"""

import sys
from pathlib import Path

# 将项目根目录加入 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.dqm.core.runner import run

if __name__ == "__main__":
    run()
