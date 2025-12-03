import os
import sys
from datetime import datetime
from typing import Optional


# ===== 路径统一：所有日志都写入项目根目录 logs/ =====
LOG_ROOT = "logs"
os.makedirs(LOG_ROOT, exist_ok=True)

_current_log_file: Optional[str] = None


def init_run_logger(market: str, mode: str) -> str:
    """
    旧接口（保持兼容性）。
    返回本次运行的日志文件路径。
    """
    ts = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    filename = f"{market}_{mode}_{ts}.log"
    full_path = os.path.join(LOG_ROOT, filename)

    global _current_log_file
    _current_log_file = full_path

    # 创建文件
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(f"[Logger] New run started at {ts}\n")

    print(f"[Logger] Using log file: {full_path}")

    return full_path


def get_logger(name: str):
    """
    保持旧框架兼容性：返回一个可调用的 log 函数。
    使用方式：
        LOG = get_logger("UnifiedRisk.AShare")
        LOG("some message")
    """
    def _inner(msg: str):
        log(f"[{name}] {msg}")

    return _inner


def log(msg: str):
    """
    写日志（打印到控制台 + 写入当前日志文件）
    """
    global _current_log_file

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"

    # 打印到控制台
    print(line)

    # 写入文件
    if _current_log_file:
        try:
            with open(_current_log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as e:
            print(f"[Logger] Failed to write to log file: {e}", file=sys.stderr)
