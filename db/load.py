from collections import deque
import datetime
import csv
import os
import argparse
import sys
from tqdm import trange, tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# 添加父目录到 Python 路径以导入 config 和 util 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config, set_warehouse_count

from util import (
    get_c_last, current_time, get_random_num, rand_dat, rand_str, get_zip_code,
    set_random_seed, rand_digit, rand_perm, get_ol_supply_w_id
)


"""
TPC-C Table Constraints and Default Values
==========================================

ITEM Table:
- I_ID: Primary key, range [1, 100000]
- I_IM_ID: Image ID, range [1, 10000]
- I_NAME: Item name, varchar(24), random string [14, 24] chars
- I_PRICE: Price, decimal(5,2), range [1.00, 100.00]
- I_DATA: Item data, varchar(50), random string [26, 50] chars, 10% contain "ORIGINAL"

WAREHOUSE Table:
- W_ID: Primary key, range [1, W] where W is number of warehouses
- W_NAME: Warehouse name, varchar(10), random string [6, 10] chars
- W_STREET_1, W_STREET_2: Address, varchar(20), random string [10, 20] chars
- W_CITY: City, varchar(20), random string [10, 20] chars
- W_STATE: State, char(2), random string 2 chars
- W_ZIP: ZIP code, char(9), format "nnnnllll" where n=digit, l=letter
- W_TAX: Tax rate, decimal(4,4), range [0.0000, 0.2000]
- W_YTD: Year-to-date balance, decimal(12,2), initial value 300000.00

STOCK Table:
- S_I_ID: Item ID, foreign key to ITEM.I_ID, range [1, 100000]
- S_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- S_QUANTITY: Stock quantity, decimal(4,0), range [10, 100]
- S_DIST_01 to S_DIST_10: District info, char(24), random string 24 chars
- S_YTD: Year-to-date, decimal(8,0), initial value 0
- S_ORDER_CNT: Order count, decimal(4,0), initial value 0
- S_REMOTE_CNT: Remote count, decimal(4,0), initial value 0
- S_DATA: Stock data, varchar(50), random string [26, 50] chars, 10% contain "ORIGINAL"

DISTRICT Table:
- D_ID: Primary key, range [1, 10] (10 districts per warehouse)
- D_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- D_NAME: District name, varchar(10), random string [6, 10] chars
- D_STREET_1, D_STREET_2: Address, varchar(20), random string [10, 20] chars
- D_CITY: City, varchar(20), random string [10, 20] chars
- D_STATE: State, char(2), random string 2 chars
- D_ZIP: ZIP code, char(9), format "nnnnllll"
- D_TAX: Tax rate, decimal(4,4), range [0.0000, 0.2000]
- D_YTD: Year-to-date balance, decimal(12,2), initial value 30000.00
- D_NEXT_O_ID: Next order ID, decimal(8,0), initial value 3001

CUSTOMER Table:
- C_ID: Primary key, range [1, 3000] (3000 customers per district)
- C_D_ID: District ID, foreign key to DISTRICT.D_ID
- C_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- C_FIRST: First name, varchar(16), random string [8, 16] chars
- C_MIDDLE: Middle initial, char(2), fixed value "OE"
- C_LAST: Last name, varchar(16), generated using NURand(255,0,999)
- C_STREET_1, C_STREET_2: Address, varchar(20), random string [10, 20] chars
- C_CITY: City, varchar(20), random string [10, 20] chars
- C_STATE: State, char(2), random string 2 chars
- C_ZIP: ZIP code, char(9), format "nnnnllll"
- C_PHONE: Phone, char(16), random numeric string 16 digits
- C_SINCE: Since date, datetime, current timestamp
- C_CREDIT: Credit, char(2), "GC" (90%) or "BC" (10%)
- C_CREDIT_LIM: Credit limit, decimal(12,2), fixed value 50000.00
- C_DISCOUNT: Discount, decimal(4,4), range [0.0000, 0.5000]
- C_BALANCE: Balance, decimal(12,2), initial value -10.00
- C_YTD_PAYMENT: YTD payment, decimal(12,2), initial value 10.00
- C_PAYMENT_CNT: Payment count, decimal(4,0), initial value 1
- C_DELIVERY_CNT: Delivery count, decimal(4,0), initial value 0
- C_DATA: Customer data, varchar(500), random string [300, 500] chars

HISTORY Table:
- H_C_ID: Customer ID, foreign key to CUSTOMER.C_ID
- H_C_D_ID: Customer district ID, foreign key to DISTRICT.D_ID
- H_C_W_ID: Customer warehouse ID, foreign key to WAREHOUSE.W_ID
- H_D_ID: District ID, foreign key to DISTRICT.D_ID
- H_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- H_DATE: Date, datetime, current timestamp
- H_AMOUNT: Amount, decimal(6,2), initial value 10.00
- H_DATA: History data, varchar(24), random string [12, 24] chars

ORDERS Table:
- O_ID: Primary key, range [1, 3000] (3000 orders per district)
- O_D_ID: District ID, foreign key to DISTRICT.D_ID
- O_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- O_C_ID: Customer ID, random permutation of [1, 3000]
- O_ENTRY_D: Entry date, datetime, current timestamp
- O_CARRIER_ID: Carrier ID, decimal(2,0), range [1, 10] for delivered orders, NULL for new orders
- O_OL_CNT: Order line count, decimal(2,0), range [5, 15]
- O_ALL_LOCAL: All local flag, decimal(1,0), 1 for all local

NEW_ORDER Table:
- NO_O_ID: Order ID, foreign key to ORDERS.O_ID, last 900 orders per district
- NO_D_ID: District ID, foreign key to DISTRICT.D_ID
- NO_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID

ORDER_LINE Table:
- OL_O_ID: Order ID, foreign key to ORDERS.O_ID
- OL_D_ID: District ID, foreign key to DISTRICT.D_ID
- OL_W_ID: Warehouse ID, foreign key to WAREHOUSE.W_ID
- OL_NUMBER: Line number, decimal(2,0), range [1, O_OL_CNT]
- OL_I_ID: Item ID, foreign key to ITEM.I_ID, range [1, 100000]
- OL_SUPPLY_W_ID: Supply warehouse ID, 99% same as OL_W_ID, 1% remote
- OL_DELIVERY_D: Delivery date, datetime, current timestamp for delivered, NULL for new
- OL_QUANTITY: Quantity, decimal(2,0), fixed value 5
- OL_AMOUNT: Amount, decimal(6,2), range [0.01, 9999.99] for delivered, 0.00 for new
- OL_DIST_INFO: District info, char(24), random string 24 chars
"""

def MakeAddress():
    """Generates a TPC-C compliant address."""
    street_1 = rand_str(10, 21)
    street_2 = rand_str(10, 21)
    city = rand_str(10, 21)
    state = rand_str(2)
    zip_code = get_zip_code()
    return street_1, street_2, city, state, zip_code

# Data Generation Functions (writing to CSV)

def load_items(output_dir="."):
    """Loads the Item table data and writes to CSV."""
    print("Loading Item ")
    filepath = os.path.join(output_dir, "item.csv")
    with open(filepath, 'w', newline='', buffering=2**20) as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(["i_id", "i_im_id", "i_name", "i_price", "i_data"])
        batch_data = []

        for i_id in trange(1, config.CNT_ITEM + 1, ncols=80, colour='green', position=1):
            i_im_id = get_random_num(1, 10000)  # [1, 10000]
            i_name = rand_str(14, 25)
            i_price = get_random_num(100, 10000) / 100.0  # [1.00, 100.00]
            i_data = rand_dat(26, 51)

            batch_data.append([i_id, i_im_id, i_name, i_price, i_data])
        writer.writerows(batch_data)
        print("\nItem Done. ")

def load_ware_work(w_id):
    batch_data2 = deque()
    # for s_i_id in trange(1, config.CNT_ITEM + 1, position=3, ncols=80, colour='blue'):
    for s_i_id in range(1, config.CNT_ITEM + 1):
        s_w_id = w_id
        s_quantity = get_random_num(10, 100)
        s_dist_01 = rand_str(24)
        s_dist_02 = rand_str(24)
        s_dist_03 = rand_str(24)
        s_dist_04 = rand_str(24)
        s_dist_05 = rand_str(24)
        s_dist_06 = rand_str(24)
        s_dist_07 = rand_str(24)
        s_dist_08 = rand_str(24)
        s_dist_09 = rand_str(24)
        s_dist_10 = rand_str(24)
        s_data = rand_dat(26, 51)

        batch_data2.append([s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, 0, 0, 0, s_data])
    return batch_data2

def load_ware(output_dir="."):
    """Loads the Warehouse, Stock, and District tables data and writes to CSV."""
    print("Loading Warehouse ")
    ware_filepath = os.path.join(output_dir, "warehouse.csv")
    stock_filepath = os.path.join(output_dir, "stock.csv")
    district_filepath = os.path.join(output_dir, "district.csv")

    with open(ware_filepath, 'w', newline='', buffering=2**20) as ware_csv, \
         open(stock_filepath, 'w', newline='', buffering=2**20) as stock_csv, \
         open(district_filepath, 'w', newline='', buffering=2**20) as district_csv:

        ware_writer = csv.writer(ware_csv)
        stock_writer = csv.writer(stock_csv)
        district_writer = csv.writer(district_csv)

        batch_data1 = deque()
        
        

        # Write headers
        ware_writer.writerow(["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"])
        stock_writer.writerow(["s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"])
        district_writer.writerow(["d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"])

        with ProcessPoolExecutor(max_workers=64) as executor:
            futures = []

            for w_id in trange(1, config.CNT_W + 1, position=2, ncols=80, colour='green'):
                # Generate Warehouse Data
                w_name = rand_str(6, 11)
                w_street_1, w_street_2, w_city, w_state, w_zip = MakeAddress()
                w_tax = get_random_num(0, 2000) / 10000.0  # [0.0000, 0.2000]
                w_ytd = 300000.00

                batch_data1.append([w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd])
                futures.append(executor.submit(load_ware_work, w_id))
                
                d_w_id = w_id
                d_ytd = 30000.0
                d_next_o_id = 3001
                batch_data3 = deque()
                for d_id in range(1, config.DIST_PER_WARE + 1):
                    d_name = rand_str(6, 11)
                    d_street_1, d_street_2, d_city, d_state, d_zip = MakeAddress()
                    d_tax = get_random_num(0, 2000) / 10000.0  # [0.0000, 0.2000]
                    batch_data3.append([d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id])
                district_writer.writerows(batch_data3) 
            ware_writer.writerows(batch_data1)

            for future in tqdm(futures, position=3, ncols=80, colour='blue'):
                batch_data2 = future.result()
                stock_writer.writerows(batch_data2)
        
        print("Warehouse Done.")


def load_cust(output_dir="."):
    """Loads the Customer and History tables data and writes to CSV."""
    print("Loading Customer ")
    cust_filepath = os.path.join(output_dir, "customer.csv")
    hist_filepath = os.path.join(output_dir, "history.csv")

    with open(cust_filepath, 'w', newline='', buffering=2**20) as cust_csv, \
         open(hist_filepath, 'w', newline='', buffering=2**20) as hist_csv:

        cust_writer = csv.writer(cust_csv)
        hist_writer = csv.writer(hist_csv)

        # Write headers
        cust_writer.writerow(["c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"])
        hist_writer.writerow(["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"])

        for w_id in trange(1, config.CNT_W + 1, position=4, ncols=80, colour='green'):
            for d_id in trange(1, config.DIST_PER_WARE + 1, position=5, ncols=80, colour='blue'):
                c_d_id = d_id
                c_w_id = w_id

                for c_id in range(1, config.CUST_PER_DIST + 1):
                    # Generate Customer Data
                    c_first = rand_str(8, 17)
                    c_middle = "OE"
                    c_last = get_c_last(c_id - 1)

                    c_street_1, c_street_2, c_city, c_state, c_zip = MakeAddress()
                    c_phone = rand_digit(16)
                    c_since = current_time()
                    c_credit = "GC" if get_random_num(0, 100) < 90 else "BC"  # 90% GC, 10% BC
                    c_credit_lim = 50000
                    c_discount = get_random_num(0, 5000) / 10000.0  # [0.0000, 0.5000]
                    c_balance = -10.0
                    c_ytd_payment = 10.0
                    c_payment_cnt = 1
                    c_delivery_cnt = 0
                    c_data = rand_str(30, 51)

                    cust_writer.writerow([c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data])

                    # Generate History Data
                    h_c_id = c_id
                    h_c_d_id = c_d_id
                    h_c_w_id = c_w_id
                    h_d_id = d_id
                    h_w_id = w_id
                    h_date = current_time()
                    h_amount = 10.0
                    h_data = rand_str(12, 25)

                    hist_writer.writerow([h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data])


def load_ord(output_dir="."):
    """Loads the Orders, New-Orders, and Order-Line tables data and writes to CSV."""
    print("Loading Orders ")
    ord_filepath = os.path.join(output_dir, "orders.csv")
    neword_filepath = os.path.join(output_dir, "new_orders.csv")
    orl_filepath = os.path.join(output_dir, "order_line.csv")

    with open(ord_filepath, 'w', newline='', buffering=2**20) as ord_csv, \
         open(neword_filepath, 'w', newline='', buffering=2**20) as neword_csv, \
         open(orl_filepath, 'w', newline='', buffering=2**20) as orl_csv:

        ord_writer = csv.writer(ord_csv)
        neword_writer = csv.writer(neword_csv)
        orl_writer = csv.writer(orl_csv)

        # Write headers
        ord_writer.writerow(["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"])
        neword_writer.writerow(["no_o_id", "no_d_id", "no_w_id"])
        orl_writer.writerow(["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"])

        for w_id in trange(1, config.CNT_W + 1, position=6, ncols=80, colour='green'):
            for d_id in trange(1, config.DIST_PER_WARE + 1, position=7, ncols=80, colour='blue'):
                o_d_id = d_id
                o_w_id = w_id

                index = 0
                perm = rand_perm(config.CUST_PER_DIST)

                for o_id in range(1, config.ORD_PER_DIST + 1):
                    # Generate Order Data
                    o_c_id = perm[index] + 1
                    index += 1
                    o_entry_d = current_time()
                    o_ol_cnt = config.ORDER_LINE_PER_ORDER
                    o_all_local = 1 # 假设所有订单都是本地订单

                    o_carrier_id = get_random_num(1, 10) if o_id <= 2100 else 0 # Undelivered orders have carrier_id 0

                    ord_writer.writerow([o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local])

                    if o_id > 2100:
                        # New-Orders table for undelivered orders
                        neword_writer.writerow([o_id, o_d_id, o_w_id])

                    # Generate Order Line Data
                    for ol_number in range(1, o_ol_cnt + 1):
                        ol_o_id = o_id
                        ol_d_id = o_d_id
                        ol_w_id = o_w_id
                        ol_i_id = get_random_num(1, config.CNT_ITEM)
                        ol_supply_w_id = get_ol_supply_w_id(o_w_id, config.CNT_W, 1)[0]
                        ol_quantity = 5
                        ol_amount = get_random_num(10, 10000) / 100.0 if o_id <= 2100 else 0.00 # Amount is 0 for undelivered
                        ol_dist_info = rand_str(24)

                        ol_delivery_d = current_time() if o_id <= 2100 else "null" # Delivery date is empty for undelivered

                        orl_writer.writerow([ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info])

        print("Orders Loading Completed for all Warehouses/Districts.")


# Main execution block
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TPCC-mysql Data Loader (Python)")
    parser.add_argument("-w", "--warehouses", type=int, required=True, help="Number of warehouses")
    parser.add_argument("-l", "--part", type=int, choices=[1, 2, 3, 4], help="Particle mode: 1=ITEMS, 2=WAREHOUSE, 3=CUSTOMER, 4=ORDERS")
    parser.add_argument("-o", "--output-dir", type=str, default=".", help="Output directory for CSV files")

    args = parser.parse_args()

    count_ware = args.warehouses
    set_warehouse_count(count_ware)
    particle_flg = args.part is not None
    part_no = args.part

    print("*************************************")
    print("*** TPCC-mysql Data Loader (Python) ***")
    print("*************************************")

    print("<Parameters>")
    print(f"  [warehouse]: {count_ware}")
    if particle_flg:
        print(f"  [part(1-4)]: {part_no}")
    print(f" [output-dir]: {args.output_dir}")

    # Seed the random number generator
    seed_val = int(datetime.datetime.now().timestamp())
    
    set_random_seed(seed_val)
    print(f"Using seed: {seed_val}")

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    print("TPCC Data Load Started...")

    if not particle_flg:
        with ProcessPoolExecutor(max_workers=4) as executor:
            f1 = executor.submit(load_items,args.output_dir)
            f2 = executor.submit(load_ware,args.output_dir)
            f3 = executor.submit(load_cust,args.output_dir)
            f4 = executor.submit(load_ord,args.output_dir)

            f1.result()
            f2.result()
            f3.result()
            f4.result()
    else:
        if part_no == 1:
            load_items(args.output_dir)
        elif part_no == 2:
            load_ware(args.output_dir)
        elif part_no == 3:
            load_cust(args.output_dir)
        elif part_no == 4:
            load_ord(args.output_dir)
        else:
            print("Unknown part_no")
            print("1:ITEMS 2:WAREHOUSE 3:CUSTOMER 4:ORDERS")

    print("\n...DATA LOADING COMPLETED SUCCESSFULLY.")