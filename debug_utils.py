import sys
import os
import threading
from datetime import datetime

# 全局开关和调用栈
_ENABLE_ = False
_PROCESS_ID = None
_LOG_FILE = None
_LOG_LOCK = threading.Lock()

def set_process_id(pid):
    """设置进程ID用于日志标识"""
    global _PROCESS_ID, _LOG_FILE
    _PROCESS_ID = pid
    os.makedirs('TPCC-Tester/result', exist_ok=True)
    _LOG_FILE = open(f'TPCC-Tester/result/process_{pid}.log', 'w', buffering=1)

def enable_log(enable: bool):
    """启用或禁用跟踪"""
    global _ENABLE_
    _ENABLE_ = enable

def print_message(color_code, msg):
    if _ENABLE_ :
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        process_prefix = f"[P_{_PROCESS_ID}]" if _PROCESS_ID else ""
        formatted_msg = f"{timestamp} {process_prefix} {msg}"
        
        # 输出到stderr（带颜色）
        print(f"\033[0m\033[1;{color_code}m{formatted_msg}\033[0m", file=sys.stderr)

def _log_message(msg):
    """统一的日志输出函数"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    process_prefix = f"[P_{_PROCESS_ID}]" if _PROCESS_ID else ""
    formatted_msg = f"{timestamp} {process_prefix} {msg}"
    # 写入日志文件（无颜色）
    if _LOG_FILE:
        with _LOG_LOCK:
            _LOG_FILE.write(f"{formatted_msg}\n")
            _LOG_FILE.flush()

def log_info(msg, *args):
    pre = "INFO: "
    formatted_msg = msg.format(*args) if args else msg
    _log_message(pre + formatted_msg)

def log_warn(msg, *args):
    pre = "WARN: "
    formatted_msg = msg.format(*args) if args else msg
    _log_message(pre + formatted_msg)

def log_error(msg, *args):
    pre = "ERROR: "
    formatted_msg = msg.format(*args) if args else msg
    _log_message(pre + formatted_msg)

def info(msg, *args):
    """打印绿色信息"""
    formatted_msg = msg.format(*args) if args else msg
    print_message("32", formatted_msg)

def warn(msg, *args):
    """打印黄色信息"""
    formatted_msg = msg.format(*args) if args else msg
    print_message("33", formatted_msg)

def error(msg, *args):
    """打印红色信息"""
    formatted_msg = msg.format(*args) if args else msg
    print_message("31", formatted_msg)

def debug_print(msg, *args):
    """调试打印函数，为白色"""
    formatted_msg = msg.format(*args) if args else msg
    print_message(37, formatted_msg)

def close_log():
    """关闭日志文件"""
    global _LOG_FILE
    if _LOG_FILE:
        _LOG_FILE.close()
        _LOG_FILE = None