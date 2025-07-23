import os
import re
import glob
from datetime import datetime

def find_and_check_duplicates(log_directory):
    """
    解析日志文件，找出已提交事务中的 'update district set d_next_o_id' 语句，
    并检查是否存在 (d_w_id, d_id, d_next_o_id) 的重复。
    最终结果按 d_w_id, d_id, timestamp 排序。

    Args:
        log_directory (str): 包含日志文件的目录路径。

    Returns:
        tuple: 一个元组，包含 (所有排序后的更新语句列表, 重复的更新语句列表)。
    """
    log_files = glob.glob(os.path.join(log_directory, 'process_rw_*.log'))
    committed_updates = []
    
    log_pattern = re.compile(r'(\d{2}:\d{2}:\d{2}\.\d{3})\s\[P_rw_\d+\]\sINFO:\s(.*)')
    update_pattern = re.compile(r"update district set d_next_o_id = (\d+) where d_id = (\d+) and d_w_id = (\d+)")

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
                # 预解析以获取排序键
                sql_match = update_pattern.search(message)
                if sql_match:
                    d_next_o_id, d_id, d_w_id = map(int, sql_match.groups())
                    timestamp_obj = datetime.strptime(timestamp_str, '%H:%M:%S.%f')
                    current_transaction_updates.append({
                        "line": line.strip(),
                        "d_w_id": d_w_id,
                        "d_id": d_id,
                        "timestamp": timestamp_obj
                    })

            elif message.strip() == 'COMMIT;' and in_transaction:
                committed_updates.extend(current_transaction_updates)
                in_transaction = False
                current_transaction_updates = []
            
            elif not message.strip().endswith(';') and in_transaction is False:
                in_transaction = False
                current_transaction_updates = []

    # --- 修改排序逻辑 ---
    # 首先按 d_w_id, 然后 d_id, 最后 timestamp 排序
    committed_updates.sort(key=lambda x: (x['d_w_id'], x['d_id'], x['timestamp']))
    
    sorted_lines = [update['line'] for update in committed_updates]
    
    # --- 重复检测逻辑 ---
    seen_updates = set()
    duplicates = []
    
    for update_line in sorted_lines:
        match = update_pattern.search(update_line)
        if match:
            d_next_o_id, d_id, d_w_id = map(int, match.groups())
            update_tuple = (d_w_id, d_id, d_next_o_id)
            
            if update_tuple in seen_updates:
                duplicates.append(update_line)
            else:
                seen_updates.add(update_tuple)
                
    return sorted_lines, duplicates

if __name__ == '__main__':
    result_dir = 'result'
    if not os.path.isdir(result_dir):
        print(f"Error: Directory '{result_dir}' not found.")
    else:
        all_updates, duplicate_updates = find_and_check_duplicates(result_dir)
        
        if not all_updates:
            print("No committed 'update district' statements found in the logs.")
            print("\nNo duplicate updates found.")
        else:
            output_file = os.path.join(result_dir, 'sorted_updates.log')
            print(f"Found {len(all_updates)} committed 'update district' statements. Saving to {output_file}")
            with open(output_file, 'w') as f:
                f.write("Found committed 'update district' statements (sorted by w_id, d_id, timestamp):\n")
                for update_line in all_updates:
                    f.write(update_line + '\n')

            if duplicate_updates:
                print(f"\n!!! Found {len(duplicate_updates)} duplicate committed update(s). Check the end of '{output_file}' for details. !!!")
                # 将重复项也写入文件末尾以便查阅
                with open(output_file, 'a') as f:
                    f.write("\n\n" + "="*50 + "\n")
                    f.write("!!! Duplicate Committed Updates (all occurrences) !!!\n")
                    # 为了清晰，我们将所有相关的重复项都找出并打印
                    all_duplicates_with_context = []
                    seen_tuples_for_dup_check = set()
                    for d_line in duplicate_updates:
                         match = re.search(r"d_next_o_id = (\d+) where d_id = (\d+) and d_w_id = (\d+)", d_line)
                         if match:
                             t = (match.group(3), match.group(2), match.group(1))
                             seen_tuples_for_dup_check.add(t)
                    
                    for update_line in all_updates:
                        match = re.search(r"d_next_o_id = (\d+) where d_id = (\d+) and d_w_id = (\d+)", update_line)
                        if match:
                            t = (match.group(3), match.group(2), match.group(1))
                            if t in seen_tuples_for_dup_check:
                                all_duplicates_with_context.append(update_line)

                    for line in sorted(all_duplicates_with_context):
                         f.write(line + '\n')
                    f.write("="*50 + "\n")
            else:
                print("\nNo duplicate updates found.")