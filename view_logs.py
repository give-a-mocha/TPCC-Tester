#!/usr/bin/env python3
"""
日志查看脚本 - 按时间排序显示所有日志文件内容
"""

import re
import sys
from datetime import datetime
from pathlib import Path

def parse_log_line(line, filename):
    """解析日志行，提取时间戳和内容"""
    # 匹配时间戳格式: HH:MM:SS.mmm
    timestamp_pattern = r'^(\d{2}:\d{2}:\d{2}\.\d{3})\s+'
    match = re.match(timestamp_pattern, line)
    if match:
        timestamp_str = match.group(1)
        # 将时间戳转换为可排序的格式
        return timestamp_str, line.strip()
    return "24:00:00.000", line.strip()

def read_all_logs(log_dir, exclude_file=None):
    """读取所有日志文件并解析"""
    log_entries = []
    log_files = []
    
    # 查找所有.log文件
    for file_path in Path(log_dir).glob('*.log'):
        # 排除输出文件
        if exclude_file and file_path.name == exclude_file:
            continue
        log_files.append(file_path)
    
    # 按文件名排序
    log_files.sort()
    
    print(f"找到 {len(log_files)} 个日志文件:")
    for log_file in log_files:
        print(f"  - {log_file.name}")
    print()
    
    # 读取每个日志文件
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():  # 跳过空行
                        timestamp, content = parse_log_line(line, log_file.name)
                        log_entries.append({
                            'timestamp': timestamp,
                            'content': content,
                        })
        except Exception as e:
            print(f"读取文件 {log_file} 时出错: {e}")
    
    return log_entries

def save_logs_to_file(log_entries, output_file, filter_level=None):
    """将排序后的日志保存到文件"""
    # 过滤日志级别
    if filter_level:
        filter_level = filter_level.upper()
        filtered_entries = []
        for entry in log_entries:
            if filter_level in entry['content']:
                filtered_entries.append(entry)
        log_entries = filtered_entries
    
    # 按时间戳排序（None值排在最后）
    log_entries.sort(key=lambda x: x['timestamp'])
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 日志汇总文件 - 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 共找到 {len(log_entries)} 条日志记录\n")
            f.write("=" * 100 + "\n\n")
            
            for entry in log_entries:
                f.write(f"{entry['content']}\n")
        
        print(f"日志已成功汇总到文件: {output_file}")
        return True
    except Exception as e:
        print(f"保存文件时出错: {e}")
        return False

def main():
    """主函数"""
    # 默认日志目录
    log_dir = Path(__file__).parent / 'result'
    
    # 解析命令行参数
    filter_level = None
    output_file = log_dir / "merged_logs.log"  # 默认输出文件名（放在result文件夹下）
    
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith('--filter='):
                filter_level = arg.split('=', 1)[1]
            elif arg.startswith('--output='):
                output_file = arg.split('=', 1)[1]
            elif arg == '--help':
                print("用法: python view_logs.py [选项]")
                print("选项:")
                print("  --filter=LEVEL   过滤指定级别的日志 (INFO, ERROR, DEBUG等)")
                print("  --output=FILE    指定输出文件名 (默认: result/merged_logs.log)")
                print("  --help           显示此帮助信息")
                print()
                print("示例:")
                print("  python view_logs.py                              # 汇总所有日志到 result/merged_logs.log")
                print("  python view_logs.py --output=all_logs.log        # 汇总所有日志到指定文件")
                print("  python view_logs.py --filter=ERROR               # 只汇总错误日志")
                return
    
    if not log_dir.exists():
        print(f"日志目录不存在: {log_dir}")
        return
    
    # 读取日志
    log_entries = read_all_logs(log_dir, Path(output_file).name)
    
    save_logs_to_file(log_entries, output_file, filter_level)

if __name__ == "__main__":
    main()
