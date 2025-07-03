import sys
import inspect
import os

# 全局开关和调用栈
_ENABLE_TRACE = False
_call_stack = []
_indent_level = 0

def enable_trace(enable: bool):
    """启用或禁用跟踪"""
    global _ENABLE_TRACE
    _ENABLE_TRACE = enable

def info(msg, *args):
    """打印绿色信息"""
    if _ENABLE_TRACE:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;32m{formatted_msg}\033[0m", file=sys.stderr)

def warn(msg, *args):
    """打印黄色信息"""
    if _ENABLE_TRACE:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;33m{formatted_msg}\033[0m", file=sys.stderr)

def error(msg, *args):
    """打印红色信息"""
    if _ENABLE_TRACE:
        formatted_msg = msg.format(*args)
        print(f"\033[0m\033[1;31m{formatted_msg}\033[0m", file=sys.stderr)

class TraceStackPrint:
    """
    用于跟踪函数调用栈的上下文管理器。
    使用 with TraceStackPrint() 进入函数，退出时自动记录。
    """
    def __init__(self):
        if not _ENABLE_TRACE:
            return

        global _indent_level
        frame = inspect.currentframe()
        if frame:
            frame = frame.f_back
        
        if frame:
            self.func_name = frame.f_code.co_name
            self.file_name = os.path.basename(frame.f_code.co_filename)
            self.line_number = frame.f_lineno
        else:
            self.func_name = "unknown"
            self.file_name = "unknown"
            self.line_number = 0

        indent_str = "  " * _indent_level
        msg = f"{indent_str}ENTER: {self.func_name}({self.file_name}:{self.line_number})"
        _call_stack.append(msg)
        info(msg)
        _indent_level += 1

    def __enter__(self):
        # __init__ 已经处理了进入逻辑
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not _ENABLE_TRACE:
            return

        global _indent_level
        _indent_level -= 1
        indent_str = "  " * _indent_level
        msg = f"{indent_str}EXIT: {self.func_name}({self.file_name}:{self.line_number})"
        _call_stack.append(msg)
        info(msg)

def print_stack():
    """打印完整的调用栈"""
    if _ENABLE_TRACE:
        info("======== Call Stack Trace ========")
        for entry in _call_stack:
            info(entry)
        info("==================================")

def clear_stack():
    """清空调用栈"""
    global _call_stack, _indent_level
    _call_stack.clear()
    _indent_level = 0

# 别名，模仿 C++ 宏
TRACE_FUNCTION = TraceStackPrint
TRACE_PRINT_STACK = print_stack
TRACE_CLEAR_STACK = clear_stack