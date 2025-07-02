import random
import string
import datetime
import csv
import sys
import os
import argparse

# Constants from tpc.h
MAXITEMS = 100000 # Corresponds to CNT_ITEM
CUST_PER_DIST = 3000
DIST_PER_WARE = 10
ORD_PER_DIST = 3000

# Based on input CNT_W (number of warehouses):
# CNT_ITEM = MAXITEMS (fixed)
# CNT_STOCK = CNT_W * MAXITEMS
# CNT_DISTRICT = CNT_W * DIST_PER_WARE
# CNT_CUSTOMER = CNT_W * DIST_PER_WARE * CUST_PER_DIST
# CNT_HISTORY = CNT_W * DIST_PER_WARE * CUST_PER_DIST
# CNT_ORDERS = CNT_W * DIST_PER_WARE * ORD_PER_DIST
# CNT_NEW_ORDERS = CNT_W * DIST_PER_WARE * 900 (for o_id > 2100 out of ORD_PER_DIST)
# CNT_ORDER_LINE = CNT_ORDERS * 10

# Constants from spt_proc.h
TIMESTAMP_LEN = 80
STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Global variables (mimicking C)
timestamp = ""
permutation = []
perm_idx = 0

# Helper functions

def SetSeed(seed_val):
    """Sets the random seed."""
    random.seed(seed_val)

def RandomNumber(min_val, max_val):
    """Generates a random number between min_val and max_val (inclusive)."""
    return random.randint(min_val, max_val)

def NURand(A, x, y):
    """Generates a non-uniform random number."""
    if A == 255:
        C = 1999
    elif A == 1023:
        C = 255
    elif A == 8191:
        C = 791
    else:
        raise ValueError("Invalid A for NURand")

    return (((RandomNumber(0, A) | RandomNumber(0, C)) % (y - x + 1)) + x)

def MakeAlphaString(min_len, max_len, prefix=""):
    """Generates a random alphabetic string."""
    length = RandomNumber(min_len, max_len)
    return prefix + ''.join(random.choice(string.ascii_letters) for _ in range(length - len(prefix)))

def MakeNumberString(min_len, max_len):
    """Generates a random numeric string."""
    length = RandomNumber(min_len, max_len)
    return ''.join(random.choice(string.digits) for _ in range(length))

def Lastname(num):
    """Generates a TPC-C compliant last name."""
    N = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
    n = N
    return n[num // 100] + n[(num // 10) % 10] + n[num % 10]

def gettimestamp():
    """Gets the current timestamp string."""
    return datetime.datetime.now().strftime(STRFTIME_FORMAT)

def InitPermutation():
    """Initializes the customer ID permutation."""
    global permutation, perm_idx
    permutation = list(range(1, CUST_PER_DIST + 1))
    random.shuffle(permutation)
    perm_idx = 0

def GetPermutation():
    """Gets the next customer ID from the permutation."""
    global permutation, perm_idx
    if perm_idx >= len(permutation):
        # Should not happen in standard TPC-C load
        raise IndexError("Permutation exhausted")
    c_id = permutation[perm_idx]
    perm_idx += 1
    return c_id

def MakeAddress():
    """Generates a TPC-C compliant address."""
    street_1 = MakeAlphaString(10, 20)
    street_2 = MakeAlphaString(10, 20)
    city = MakeAlphaString(10, 20)
    state = MakeAlphaString(2, 2)
    zip_code = MakeNumberString(9, 9)
    return street_1, street_2, city, state, zip_code

# Data Generation Functions (writing to CSV)

def load_items(output_dir="."):
    """Loads the Item table data and writes to CSV."""
    print("Loading Item ")
    filepath = os.path.join(output_dir, "item.csv")
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(["i_id", "i_im_id", "i_name", "i_price", "i_data"])

        orig = [0] * (MAXITEMS + 1)
        for _ in range(MAXITEMS // 10):
            pos = RandomNumber(1, MAXITEMS)
            while orig[pos]:
                pos = RandomNumber(1, MAXITEMS)
            orig[pos] = 1

        for i_id in range(1, MAXITEMS + 1):
            i_im_id = RandomNumber(1, 10000)
            i_name = MakeAlphaString(14, 24)
            i_price = RandomNumber(100, 10000) / 100.0
            i_data = MakeAlphaString(26, 50)

            if orig[i_id]:
                pos = RandomNumber(0, len(i_data) - 8)
                i_data = i_data[:pos] + "original" + i_data[pos+8:]

            writer.writerow([i_id, i_im_id, i_name, i_price, i_data])

            if i_id % 100 == 0:
                print(".", end='', flush=True)
                if i_id % 5000 == 0:
                    print(f" {i_id}", flush=True)
        print("\nItem Done. ")

def load_ware(min_ware, max_ware, output_dir="."):
    """Loads the Warehouse, Stock, and District tables data and writes to CSV."""
    print("Loading Warehouse ")
    ware_filepath = os.path.join(output_dir, "warehouse.csv")
    stock_filepath = os.path.join(output_dir, "stock.csv")
    district_filepath = os.path.join(output_dir, "district.csv")

    with open(ware_filepath, 'w', newline='') as ware_csv, \
         open(stock_filepath, 'w', newline='') as stock_csv, \
         open(district_filepath, 'w', newline='') as district_csv:

        ware_writer = csv.writer(ware_csv)
        stock_writer = csv.writer(stock_csv)
        district_writer = csv.writer(district_csv)

        # Write headers
        ware_writer.writerow(["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"])
        stock_writer.writerow(["s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"])
        district_writer.writerow(["d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"])

        for w_id in range(min_ware, max_ware + 1):
            # Generate Warehouse Data
            w_name = MakeAlphaString(6, 10)
            w_street_1, w_street_2, w_city, w_state, w_zip = MakeAddress()
            w_tax = RandomNumber(10, 20) / 100.0
            w_ytd = 300000.00

            ware_writer.writerow([w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd])

            # Generate Stock Data for this warehouse
            print(f"Loading Stock Wid={w_id}")
            orig = [0] * (MAXITEMS + 1)
            for _ in range(MAXITEMS // 10):
                pos = RandomNumber(1, MAXITEMS)
                while orig[pos]:
                    pos = RandomNumber(1, MAXITEMS)
                orig[pos] = 1

            for s_i_id in range(1, MAXITEMS + 1):
                s_w_id = w_id
                s_quantity = RandomNumber(10, 100)
                s_dist_01 = MakeAlphaString(24, 24)
                s_dist_02 = MakeAlphaString(24, 24)
                s_dist_03 = MakeAlphaString(24, 24)
                s_dist_04 = MakeAlphaString(24, 24)
                s_dist_05 = MakeAlphaString(24, 24)
                s_dist_06 = MakeAlphaString(24, 24)
                s_dist_07 = MakeAlphaString(24, 24)
                s_dist_08 = MakeAlphaString(24, 24)
                s_dist_09 = MakeAlphaString(24, 24)
                s_dist_10 = MakeAlphaString(24, 24)
                s_data = MakeAlphaString(26, 50)

                if orig[s_i_id]:
                    pos = RandomNumber(0, len(s_data) - 8)
                    s_data = s_data[:pos] + "original" + s_data[pos+8:]

                stock_writer.writerow([s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, 0, 0, 0, s_data])

                if s_i_id % 100 == 0:
                    print(".", end='', flush=True)
                    if s_i_id % 5000 == 0:
                        print(f" {s_i_id}", flush=True)
            print(" Stock Done.")

            # Generate District Data for this warehouse
            print("Loading District")
            d_w_id = w_id
            d_ytd = 30000.0
            d_next_o_id = 3001

            for d_id in range(1, DIST_PER_WARE + 1):
                d_name = MakeAlphaString(6, 10)
                d_street_1, d_street_2, d_city, d_state, d_zip = MakeAddress()
                d_tax = RandomNumber(10, 20) / 100.0

                district_writer.writerow([d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id])

            print(" District Done.")
        print("Warehouse Done.")


def load_cust(min_ware, max_ware, output_dir="."):
    """Loads the Customer and History tables data and writes to CSV."""
    print("Loading Customer ")
    cust_filepath = os.path.join(output_dir, "customer.csv")
    hist_filepath = os.path.join(output_dir, "history.csv")

    with open(cust_filepath, 'w', newline='') as cust_csv, \
         open(hist_filepath, 'w', newline='') as hist_csv:

        cust_writer = csv.writer(cust_csv)
        hist_writer = csv.writer(hist_csv)

        # Write headers
        cust_writer.writerow(["c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"])
        hist_writer.writerow(["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"])

        for w_id in range(min_ware, max_ware + 1):
            for d_id in range(1, DIST_PER_WARE + 1):
                print(f"Loading Customer for DID={d_id}, WID={w_id}")
                c_d_id = d_id
                c_w_id = w_id

                for c_id in range(1, CUST_PER_DIST + 1):
                    # Generate Customer Data
                    c_first = MakeAlphaString(8, 16)
                    c_middle = "OE"
                    if c_id <= 1000:
                        c_last = Lastname(c_id - 1)
                    else:
                        c_last = Lastname(NURand(255, 0, 999))

                    c_street_1, c_street_2, c_city, c_state, c_zip = MakeAddress()
                    c_phone = MakeNumberString(16, 16)
                    c_since = gettimestamp()
                    c_credit = "GC" if RandomNumber(0, 1) else "BC"
                    c_credit_lim = 50000
                    c_discount = RandomNumber(0, 50) / 100.0
                    c_balance = -10.0
                    c_ytd_payment = 10.0
                    c_payment_cnt = 1
                    c_delivery_cnt = 0
                    c_data = MakeAlphaString(30, 50)

                    cust_writer.writerow([c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data])

                    # Generate History Data
                    h_c_id = c_id
                    h_c_d_id = c_d_id
                    h_c_w_id = c_w_id
                    h_d_id = d_id
                    h_w_id = w_id
                    h_date = gettimestamp()
                    h_amount = 10.0
                    h_data = MakeAlphaString(12, 24)

                    hist_writer.writerow([h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data])

                    if c_id % 100 == 0:
                        print(".", end='', flush=True)
                        if c_id % 1000 == 0:
                            print(f" {c_id}", flush=True)
                print("Customer Done.")
        print("Customer Loading Completed for all Warehouses/Districts.")


def load_ord(min_ware, max_ware, output_dir="."):
    """Loads the Orders, New-Orders, and Order-Line tables data and writes to CSV."""
    print("Loading Orders ")
    ord_filepath = os.path.join(output_dir, "orders.csv")
    neword_filepath = os.path.join(output_dir, "new_orders.csv")
    orl_filepath = os.path.join(output_dir, "order_line.csv")

    with open(ord_filepath, 'w', newline='') as ord_csv, \
         open(neword_filepath, 'w', newline='') as neword_csv, \
         open(orl_filepath, 'w', newline='') as orl_csv:

        ord_writer = csv.writer(ord_csv)
        neword_writer = csv.writer(neword_csv)
        orl_writer = csv.writer(orl_csv)

        # Write headers
        ord_writer.writerow(["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"])
        neword_writer.writerow(["no_o_id", "no_d_id", "no_w_id"])
        orl_writer.writerow(["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"])

        for w_id in range(min_ware, max_ware + 1):
            for d_id in range(1, DIST_PER_WARE + 1):
                print(f"Loading Orders for D={d_id}, W={w_id}")
                o_d_id = d_id
                o_w_id = w_id

                InitPermutation() # Initialize permutation for customers in this district

                for o_id in range(1, ORD_PER_DIST + 1):
                    # Generate Order Data
                    o_c_id = GetPermutation()
                    o_entry_d = gettimestamp()
                    o_ol_cnt = 10
                    o_all_local = 1 # Assuming all orders are local based on C code

                    o_carrier_id = RandomNumber(1, 10) if o_id <= 2100 else 0 # Undelivered orders have carrier_id 0

                    ord_writer.writerow([o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local])

                    if o_id > 2100:
                        # New-Orders table for undelivered orders
                        neword_writer.writerow([o_id, o_d_id, o_w_id])

                    # Generate Order Line Data
                    for ol_number in range(1, o_ol_cnt + 1):
                        ol_o_id = o_id
                        ol_d_id = o_d_id
                        ol_w_id = o_w_id
                        ol_i_id = RandomNumber(1, MAXITEMS)
                        ol_supply_w_id = o_w_id # Assuming supply warehouse is the same as order warehouse
                        ol_quantity = 5
                        ol_amount = RandomNumber(10, 10000) / 100.0 if o_id <= 2100 else 0.00 # Amount is 0 for undelivered
                        ol_dist_info = MakeAlphaString(24, 24)

                        ol_delivery_d = gettimestamp() if o_id <= 2100 else "" # Delivery date is empty for undelivered

                        orl_writer.writerow([ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info])

                    if o_id % 100 == 0:
                        print(".", end='', flush=True)
                        if o_id % 1000 == 0:
                            print(f" {o_id}", flush=True)
                print("Orders Done.")
        print("Orders Loading Completed for all Warehouses/Districts.")


# Main execution block
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TPCC-mysql Data Loader (Python)")
    parser.add_argument("-w", "--warehouses", type=int, required=True, help="Number of warehouses")
    parser.add_argument("-l", "--part", type=int, choices=[1, 2, 3, 4], help="Particle mode: 1=ITEMS, 2=WAREHOUSE, 3=CUSTOMER, 4=ORDERS")
    parser.add_argument("-m", "--min-wh", type=int, default=1, help="Minimum warehouse ID (for particle mode)")
    parser.add_argument("-n", "--max-wh", type=int, help="Maximum warehouse ID (for particle mode)")
    parser.add_argument("-o", "--output-dir", type=str, default=".", help="Output directory for CSV files")

    args = parser.parse_args()

    count_ware = args.warehouses
    particle_flg = args.part is not None
    part_no = args.part
    min_ware = args.min_wh
    max_ware = args.max_wh if args.max_wh is not None else count_ware

    if not particle_flg:
        min_ware = 1
        max_ware = count_ware

    print("*************************************")
    print("*** TPCC-mysql Data Loader (Python) ***")
    print("*************************************")

    print("<Parameters>")
    print(f"  [warehouse]: {count_ware}")
    if particle_flg:
        print(f"  [part(1-4)]: {part_no}")
        print(f"     [MIN WH]: {min_ware}")
        print(f"     [MAX WH]: {max_ware}")
    print(f" [output-dir]: {args.output_dir}")

    # Seed the random number generator
    seed_val = None
    try:
        with open("/dev/urandom", "rb") as f:
            seed_val = int.from_bytes(f.read(4), byteorder='big')
    except FileNotFoundError:
        try:
            with open("/dev/random", "rb") as f:
                 seed_val = int.from_bytes(f.read(4), byteorder='big')
        except FileNotFoundError:
            seed_val = int(datetime.datetime.now().timestamp())
            print("Warning: /dev/urandom and /dev/random not found. Using time-based seed.")

    SetSeed(seed_val)
    print(f"Using seed: {seed_val}")

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    print("TPCC Data Load Started...")

    if not particle_flg:
        load_items(args.output_dir)
        load_ware(min_ware, max_ware, args.output_dir)
        load_cust(min_ware, max_ware, args.output_dir)
        load_ord(min_ware, max_ware, args.output_dir)
    else:
        if part_no == 1:
            load_items(args.output_dir)
        elif part_no == 2:
            load_ware(min_ware, max_ware, args.output_dir)
        elif part_no == 3:
            load_cust(min_ware, max_ware, args.output_dir)
        elif part_no == 4:
            load_ord(min_ware, max_ware, args.output_dir)
        else:
            print("Unknown part_no")
            print("1:ITEMS 2:WAREHOUSE 3:CUSTOMER 4:ORDERS")

    print("\n...DATA LOADING COMPLETED SUCCESSFULLY.")