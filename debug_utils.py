import sys

# 全局开关和调用栈
_ENABLE_ = False

def enable_trace(enable: bool):
    """启用或禁用跟踪"""
    global _ENABLE_
    _ENABLE_ = enable

def enable_stack_trace(enable: bool):
    """启用或禁用调用栈跟踪"""
    global _ENABLE_STACK_TRACE
    _ENABLE_STACK_TRACE = enable

def info(msg, *args):
    """打印绿色信息"""
    if _ENABLE_:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;32m{formatted_msg}\033[0m", file=sys.stderr)

def warn(msg, *args):
    """打印黄色信息"""
    if _ENABLE_:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;33m{formatted_msg}\033[0m", file=sys.stderr)

def error(msg, *args):
    """打印红色信息"""
    if _ENABLE_:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;31m{formatted_msg}\033[0m", file=sys.stderr)