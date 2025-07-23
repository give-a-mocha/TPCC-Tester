import os
import re
import glob
from datetime import datetime

def find_and_check_duplicates(log_directory):
    """
    解析日志文件，找出已提交事务中的 'update district set d_next_o_id' 语句，
    并检查是否存在 (d_w_id, d_id, d_next_o_id) 的重复。

    Args:
        log_directory (str): 包含日志文件的目录路径。

    Returns:
        tuple: 一个元组，包含 (所有排序后的更新语句列表, 重复的更新语句列表)。
    """
    log_files = glob.glob(os.path.join(log_directory, 'process_rw_*.log'))
    committed_updates = []
    
    log_pattern = re.compile(r'(\d{2}:\d{2}:\d{2}\.\d{3})\s\[P_rw_\d+\]\sINFO:\s(.*)')

    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                lines = f.readlines()
        except IOError as e:
            print(f"Error reading file {log_file}: {e}")
            continue

        in_transaction = False
        current_transaction_updates = []

        for line in lines:
            match = log_pattern.match(line)
            if not match:
                continue

            timestamp_str, message = match.groups()
            
            if message.strip() == 'BEGIN;':
                in_transaction = True
                current_transaction_updates = []
            
            elif 'update district set d_next_o_id' in message and in_transaction:
                current_transaction_updates.append((timestamp_str, line.strip()))

            elif message.strip() == 'COMMIT;' and in_transaction:
                committed_updates.extend(current_transaction_updates)
                in_transaction = False
                current_transaction_updates = []
            
            elif not message.strip().endswith(';') and in_transaction is False:
                in_transaction = False
                current_transaction_updates = []

    committed_updates.sort(key=lambda x: datetime.strptime(x[0], '%H:%M:%S.%f'))
    
    sorted_updates = [update[1] for update in committed_updates]
    
    # --- 新增的重复检测逻辑 ---
    from collections import defaultdict
    update_map = defaultdict(list)
    # 正则表达式用于从SQL语句中提取ID
    update_pattern = re.compile(r"update district set d_next_o_id = (\d+) where d_id = (\d+) and d_w_id = (\d+)")

    for update_line in sorted_updates:
        match = update_pattern.search(update_line)
        if match:
            d_next_o_id, d_id, d_w_id = map(int, match.groups())
            update_tuple = (d_w_id, d_id, d_next_o_id)
            update_map[update_tuple].append(update_line)

    all_duplicate_lines = []
    for lines in update_map.values():
        if len(lines) > 1:
            all_duplicate_lines.extend(lines)
            
    return sorted_updates, all_duplicate_lines

if __name__ == '__main__':
    result_dir = 'result'
    if not os.path.isdir(result_dir):
        print(f"Error: Directory '{result_dir}' not found.")
    else:
        output_file = os.path.join(result_dir, 'sorted_updates.log')
        all_updates, duplicate_updates = find_and_check_duplicates(result_dir)
        
        if all_updates:
            print(f"Found {len(all_updates)} committed 'update district' statements. Saving to {output_file}")
            with open(output_file, 'w') as f:
                f.write("Found committed 'update district' statements:\n")
                for update_line in all_updates:
                    f.write(update_line + '\n')
        else:
            print("No committed 'update district' statements found in the logs.")

        if duplicate_updates:
            print("\n" + "="*50)
            print(f"!!! Found duplicate committed updates. See details in {output_file}. !!!")
            print("="*50)

            # Append duplicates to the log file
            with open(output_file, 'a') as f:
                f.write("\n\n" + "="*50 + "\n")
                f.write("!!! Duplicate Committed Updates (all occurrences) !!!\n")
                for update_line in duplicate_updates:
                    f.write(update_line + '\n')
                f.write("="*50 + "\n")
        else:
            print("\nNo duplicate updates found.")