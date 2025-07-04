import re
from typing import List, Union

from debug_utils import log_error, error

from db.rmdb_client import Client
from mysql.sql import (
    select, insert, update, delete, eq, lt, beq, SQLState
)
from db.table_layouts import (
    WAREHOUSE,W_ID,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_TAX,W_YTD,
    STOCK,S_I_ID,S_W_ID,S_QUANTITY,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,
    S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,
    DISTRICT,D_ID,D_W_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_TAX,D_YTD,D_NEXT_O_ID,
    CUSTOMER,C_ID,C_D_ID,C_W_ID,C_LAST,C_MIDDLE,C_FIRST,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,
    C_PHONE,C_SINCE,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_DATA,
    HISTORY,
    NEW_ORDERS,NO_O_ID,NO_D_ID,NO_W_ID,
    ORDERS,O_ID,O_D_ID,O_W_ID,O_C_ID,O_ENTRY_D,O_CARRIER_ID,O_OL_CNT,
    ORDER_LINE,OL_O_ID,OL_D_ID,OL_W_ID,OL_I_ID,OL_SUPPLY_W_ID,OL_DELIVERY_D,OL_QUANTITY,OL_AMOUNT,
    ITEM,I_ID,I_NAME,I_PRICE,I_DATA,
    COUNT,MAX,MIN,SUM
)

from util import (
    current_time
)
from config import config

class Driver:
    def __init__(self, scale):
        self._scale = scale    
        self._client = Client()
        self._flag = True
        # self._delivery_q = Queue()
        # self._delivery_t = Thread(target=self.process_delivery, args=(self._delivery_q,))
        # self._delivery_t.start()
        # self._delivery_stop = False

    def delay_close(self):
        self._flag = False
        # while not self._delivery_stop:
        #     continue
        self._client.close()

    # def close(self):
    #     self._client.close()

    def build(self):
        print("Build table schema...")
        sql = open("TPCC-Tester/db/create_tables.sql", "r").read().split('\n')
        for line in sql:
            if line:
                self._client.send_cmd(line)

    def load(self):
        print("Load table data...")
        sql = open("TPCC-Tester/db/load_csvs.sql", "r").read().split('\n')
        for line in sql:
            if line:
                self._client.send_cmd(line)
        print('Database has been initialized.')

    def create_index(self):
        print("Create index...")
        sql = open("TPCC-Tester/db/create_index.sql", "r").read().split('\n')
        for line in sql:
            if line:
                self._client.send_cmd(line)

    def all_in_load(self):
        print("Loading data...")
        sql = open("TPCC-Tester/db/load_data.sql", "r").read().split('\n')
        for line in sql:
            if line:
                self._client.send_cmd(line)

    def count_and_check(self, client, table:str, count_as:str, expected_count:int, count_type:str):
        """
        A helper function to count and check the result.

        :param client: The database client.
        :param table: The table to perform the count on.
        :param count_as: The alias for the count column.
        :param expected_count: The expected count value.
        :param count_type: Descriptive type of count for error messages.
        :return: None
        """
        count_result = 0
        res = select(client=client, table=[table], col=[COUNT(alias=count_as)])
        if(res is None) :
            print(f'error, {count_type}: select is failed or empty')
            return 
        try:
            count_result = eval(res[0][0])
        except IndexError:
            print(f'error, {count_type}: index error in select result')
            return
        if count_result != expected_count:
            print(f'failed, {count_type}: {count_result}, expecting: {expected_count}')

    def count_star(self):
        print("Count star...")
        # 遍历每个表的信息并进行检查
        tables_error = config.get_tables_info()
        for table, count_as, expected_count, count_type in tables_error:
            self.count_and_check(self._client, table, count_as, expected_count, count_type)

    def consistency_check(self):
        print("consistency checking...")

        w_id = 0
        d_id = 0

        try:
            for w_id in range(1, config.W_ID_MAX):
                for d_id in range(1, config.D_ID_MAX):
                    res = select(client=self._client,
                                 table=[DISTRICT],
                                 col=[D_NEXT_O_ID],  
                                 where=[(D_ID, eq, d_id),
                                        (D_W_ID, eq, w_id)])
                    if res is None:
                        print(f"error select {DISTRICT} is failed or empty")
                        return

                    d_next_o_id = eval(res[0][0])

                    res = select(client=self._client,
                                 table=[ORDERS],
                                 col=[MAX(O_ID)],
                                 where=[(O_W_ID, eq, w_id),
                                        (O_D_ID, eq, d_id)])

                    if res is None :
                        print(f"error select {ORDERS} is failed or empty: {w_id}, {d_id}")
                        return

                    max_o_id = eval(res[0][0])

                    res = select(client=self._client,
                                 table=[NEW_ORDERS],
                                 col=[MAX(NO_O_ID)],
                                 where=[(NO_W_ID, eq, w_id),
                                        (NO_D_ID, eq, d_id)])

                    if res is None:
                        print(f"error select {NEW_ORDERS} is failed or empty: {w_id}, {d_id}")
                        return

                    max_no_o_id = eval(res[0][0])

                    if d_next_o_id - 1 != max_o_id or d_next_o_id - 1 != max_no_o_id:
                        print(f"d_next_o_id={d_next_o_id}, max(o_id)={max_o_id}, max(no_o_id)={max_no_o_id} when d_id={d_id} and w_id={w_id}")
                        return
                    
            print("consistency check for district, orders and new_orders pass!")

            for w_id in range(1, config.W_ID_MAX):
                for d_id in range(1, config.D_ID_MAX):
                    res = select(client=self._client,
                                 table=[NEW_ORDERS],
                                 col=[COUNT(NO_O_ID)],
                                 where=[(NO_W_ID, eq, w_id),
                                        (NO_D_ID, eq, d_id)])
                    if res is None:
                        print(f"error select {NEW_ORDERS} is failed or empty: {w_id}, {d_id}")
                        return
                    
                    num_no_o_id = eval(res[0][0])

                    res = select(client=self._client,
                                 table=[NEW_ORDERS],
                                 col=[MAX(NO_O_ID)],
                                 where=[(NO_W_ID, eq, w_id),
                                        (NO_D_ID, eq, d_id)])
                    
                    if res is None:
                        print(f"error select {NEW_ORDERS} is failed or empty: {w_id}, {d_id}")
                        return

                    max_no_o_id = eval(res[0][0])

                    res = select(client=self._client,
                                 table=[NEW_ORDERS],
                                 col=[MIN(NO_O_ID)],
                                 where=[(NO_W_ID, eq, w_id),
                                        (NO_D_ID, eq, d_id)])
                    if res is None:
                        print(f"error select {NEW_ORDERS} is failed or empty: {w_id}, {d_id}")
                        return
                    
                    min_no_o_id = eval(res[0][0])

                    if num_no_o_id != max_no_o_id - min_no_o_id + 1:
                        print(f"count(no_o_id)={num_no_o_id}, max(no_o_id)={max_no_o_id}, min(no_o_id)={min_no_o_id} when d_id={d_id} and w_id={w_id}")
                        return                 
            print("consistency check for new_orders pass!")

            for w_id in range(1, config.W_ID_MAX):
                for d_id in range(1, config.D_ID_MAX):
                    res = select(client=self._client,
                                 table=[ORDERS],
                                 col=[SUM(O_OL_CNT)],
                                 where=[(O_W_ID, eq, w_id),
                                         (O_D_ID, eq, d_id)])
                    
                    if res is None:
                        print(f"error select {ORDERS} is failed or empty: {w_id}, {d_id}")
                        return

                    sum_o_ol_cnt = eval(res[0][0])

                    res = select(client=self._client,
                                 table=[ORDER_LINE],
                                 col=[COUNT(OL_O_ID)],
                                 where=[(OL_W_ID, eq, w_id),
                                    (OL_D_ID, eq, d_id)])

                    if res is None:
                        print(f"error select {ORDER_LINE} is failed or empty: {w_id}, {d_id}")
                        return
                
                    num_ol_o_id = eval(res[0][0])

                    if sum_o_ol_cnt != num_ol_o_id:
                        print(f"sum(o_ol_cnt)={sum_o_ol_cnt}, count(ol_o_id)={num_ol_o_id} when d_id={d_id} and w_id={w_id}")
                        return 
            print("consistency check for orders and order_line pass!")

        except Exception as e:
            print(f"Exception occurred in w_id: {w_id}, d_id: {d_id}")
            print(str(e))

    def consistency_check2(self, cnt_new_orders:int):
        print("consistency checking 2...")
        try:
            res = select(client=self._client,
                            table=[ORDERS],
                            col=[COUNT(alias='count_orders')],
                            )
            
            if res is None:
                print("consistency checking 2 error!")
                print(f"error select {ORDERS} is failed or empty")
                return
            cnt_orders = eval(res[0][0])
            if cnt_orders != config.CNT_ORDERS + cnt_new_orders:
                print("consistency checking 2 error!")
                print(f"count(*)={cnt_orders}, count(new_orders)={cnt_new_orders} when origin orders={config.CNT_ORDERS}")
                return 
            print("all pass!")
        except Exception as e:
            print(str(e))

    def do_new_order(self, w_id:int, d_id:int, c_id:int, ol_i_id:List[int], ol_supply_w_id:List[int], ol_quantity:List[int]) -> SQLState:
        '''
        功能: 处理新订单创建 三个阶段:

        Phase 1: 获取仓库税率、区域税率和下一个订单ID
        Phase 2: 插入 ORDERS 和 NEW_ORDERS 记录
        Phase 3: 处理每个订单行项目
        查询商品信息 (ITEM 表)
        更新库存信息 (STOCK 表)
        插入订单行 (ORDER_LINE 表)
        '''
        res = []
        ol_cnt = len(ol_i_id)
        ol_amount = 0
        total_amount = 0
        brand_generic = ''
        s_data = ''

        # transcation
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ New Order')
        # phase 1
        # 检索仓库（warehouse）税率、区域（district）税率和下一个可用订单号。
        try:
            # 查询区域的税率和下一个订单号
            res = select(client=self._client,
                            table=[DISTRICT],
                            col=[D_TAX, D_NEXT_O_ID],
                            where=[(D_ID, eq, d_id),
                                (D_W_ID, eq, w_id)])

            # 每一个都加上判断
            if res is None:
                log_error("select from DISTRICT failed or empty")
                error("select from DISTRICT failed or empty")
                exit(1)
                return SQLState.ABORT

            d_tax, d_next_o_id = res[0]
            d_tax = eval(d_tax)
            d_next_o_id = eval(d_next_o_id)
        except Exception as e:
            print(f"Exception occurred in w_id: {w_id}, d_id: {d_id}, res: {res}")
            print(e)
            # exit(1)
            d_tax = 0
            d_next_o_id = 0

        # 更新区域的下一个订单号
        if update(client=self._client,
                    table=DISTRICT,
                    set=[(D_NEXT_O_ID, d_next_o_id + 1)],
                    where=[(D_ID, eq, d_id), (D_W_ID, eq, w_id)]) == SQLState.ABORT:
            return SQLState.ABORT

        try:
            # 查询仓库的税率和客户的余额
            res = select(client=self._client,
                            table=[CUSTOMER, WAREHOUSE],
                            col=[C_DISCOUNT, C_LAST, C_CREDIT, W_TAX],
                            where=[(W_ID, eq, w_id), (C_W_ID, eq, W_ID), (C_D_ID, eq, d_id), (C_ID, eq, c_id)]
                            )
            if res is None:
                log_error("select from CUSTOMER and WAREHOUSE failed or empty")
                error("select from CUSTOMER and WAREHOUSE failed or empty")
                exit(1)
                return SQLState.ABORT
            c_discount, c_last_, c_credit, w_tax = res[0]
            c_discount = eval(c_discount)
            w_tax = eval(w_tax)
        except Exception as e:
            print(f'error {CUSTOMER}, {WAREHOUSE}: {str(e)}')
            # exit(1)
            c_discount = 0
            w_tax = 0

        # phase 2
        # 插入订单（order）、新订单（new-order）和新订单行（order-line）。
        order_time = "'" + current_time() + "'"
        if insert(client=self._client,
                    table=ORDERS,
                    rows=[d_next_o_id, d_id, w_id, c_id, order_time, 0, ol_cnt,
                        int(len(set(ol_supply_w_id)) == 1)]) == SQLState.ABORT:
            return SQLState.ABORT

        if insert(client=self._client,
                    table=NEW_ORDERS,
                    rows=[d_next_o_id, d_id, w_id]) == SQLState.ABORT:
            return SQLState.ABORT

        # phase 3
        # 处理每个订单行项目
        for i in range(ol_cnt):
            try:
                # 查询商品（item）信息
                res = select(client=self._client,
                                table=[ITEM],
                                col=[I_PRICE, I_NAME, I_DATA],
                                where=[(I_ID, eq, ol_i_id[i])])
                if res is None:
                    log_error("select from ITEM failed or empty")
                    error("select from ITEM failed or empty")
                    exit(1)
                    return SQLState.ABORT
                i_price, i_name, i_data = res[0]
                i_price = eval(i_price)
            except Exception as e:
                print(f'error {ITEM}: {str(e)}')
                # exit(1)
                i_price = 1
                i_data = 'null'

            try:
                # 查询库存（stock）信息
                res = select(client=self._client,
                                table=[STOCK],
                                col=[
                                    S_QUANTITY, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06,
                                    S_DIST_07,
                                    S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA],
                                where=[(S_I_ID, eq, ol_i_id[i]),
                                    (S_W_ID, eq, ol_supply_w_id[i])])
                if res is None:
                    log_error("Error: select from STOCK failed or empty")
                    error("Error: select from STOCK failed or empty")
                    exit(1)
                    return SQLState.ABORT
                s_quantity, *s_dist, s_ytd, s_order_cnt, s_remote_cnt, s_data = res[0]
                s_quantity = eval(s_quantity)
                s_ytd = eval(s_ytd)
                s_order_cnt = eval(s_order_cnt)
                s_remote_cnt = eval(s_remote_cnt)
            except Exception as e:
                print(f'error {STOCK}: {str(e)}')
                # exit(1)
                s_quantity = 0
                s_ytd = 0
                s_order_cnt = 0
                s_remote_cnt = 0
                s_dist = []

            if s_quantity - ol_quantity[i] >= 10:
                s_quantity -= ol_quantity[i]
            else:
                s_quantity = s_quantity - ol_quantity[i] + 91

            s_ytd += ol_quantity[i]
            s_order_cnt += 1

            if ol_supply_w_id[i] != w_id:
                s_remote_cnt += 1
            # 更新库存信息
            if update(client=self._client,
                        table=STOCK,
                        set=[(S_QUANTITY, s_quantity),
                            (S_YTD, s_ytd),
                            (S_ORDER_CNT, s_order_cnt),
                            (S_REMOTE_CNT, s_remote_cnt)],
                        where=[(S_I_ID, eq, ol_i_id[i]),
                                (S_W_ID, eq, ol_supply_w_id[i])]) == SQLState.ABORT:
                return SQLState.ABORT
            ol_amount = ol_quantity[i] * i_price
            brand_generic = 'B' if re.search('ORIGINAL', i_data) and re.search('ORIGINAL', s_data) else 'G'

            try:
                # 插入订单行（order-line）信息
                if insert(client=self._client,
                            table=ORDER_LINE,
                            rows=[d_next_o_id, d_id, w_id, i, ol_i_id[i], ol_supply_w_id[i], order_time, ol_quantity[i],
                                ol_amount, "'" + s_dist[d_id - 1] + "'"]) == SQLState.ABORT:
                    return SQLState.ABORT

            except Exception as e:
                print(f'error {ORDER_LINE}: {str(e)}')
                pass

            total_amount += ol_amount

        total_amount *= (1 - c_discount) * (1 + w_tax + d_tax)

        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- New Order')
        return SQLState.SUCCESS
    def do_payment(self, w_id:int, d_id:int, c_w_id:int, c_d_id:int, c_query:Union[str, int], h_amount:float) -> SQLState:
        '''
        功能: 处理客户付款 主要步骤:
        更新仓库年度销售额 (WAREHOUSE.W_YTD)
        更新区域年度销售额 (DISTRICT.D_YTD)
        根据客户ID或姓名查找客户
        更新客户余额和付款信息
        如果是信用不良客户，更新客户数据
        插入历史记录 (HISTORY 表)
        '''
        c_balance = 0
        c_ytd_payment = 0
        c_payment_cnt = 0
        c_credit = 'GC'
        c_id = 0
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Payment')
        try:
            # 查询仓库信息
            res = select(client=self._client,
                            table=[WAREHOUSE],
                            col=[W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD],
                            where=[(W_ID, eq, w_id)])
            if res is None:
                log_error("Error: select from WAREHOUSE failed or empty")
                error("Error: select from WAREHOUSE failed or empty")
                exit(1)
                return SQLState.ABORT
            w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd = res[0]
        except Exception as e:
            w_name, d_name = 'null', 'null'
        # w_ytd = eval(w_ytd)
        # 更新仓库年度销售额
        if update(client=self._client,
                    table=WAREHOUSE,
                    set=[(W_YTD, W_YTD + ' + ' + str(h_amount))],
                    where=[(W_ID, eq, w_id)]) == SQLState.ABORT:
            return SQLState.ABORT
        try:
            # 查询区域信息
            res = select(client=self._client,
                            table=[DISTRICT],
                            col=[D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD],
                            where=[(D_W_ID, eq, w_id), (D_ID, eq, d_id)])
            if res is None:
                log_error("Error: select from DISTRICT failed or empty")
                error("Error: select from DISTRICT failed or empty")
                exit(1)
                return SQLState.ABORT
            d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd = res[0]
        except Exception as e:
            d_name = 'null'
        # d_ytd = eval(d_ytd)
        # 更新区域年度销售额
        if update(client=self._client,
                    table=DISTRICT,
                    set=[(D_YTD, D_YTD + ' + ' + str(h_amount))],
                    where=[(D_W_ID, eq, w_id), (D_ID, eq, d_id)]) == SQLState.ABORT:
            return SQLState.ABORT

        # 根据客户ID或姓名查找客户
        if type(c_query) is str:
            c_query = "'" + c_query + "'"
            try:
                result = select(client=self._client,
                                table=[CUSTOMER],
                                col=[C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                        C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                        C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT],
                                where=[(C_LAST, eq, c_query),
                                        (C_W_ID, eq, c_w_id),
                                        (C_D_ID, eq, c_d_id)],
                                # order_by=C_FIRST,
                                # asc=True
                                )
                if result is None:
                    return SQLState.ABORT
                result = result[0]
            except Exception as e:
                c_credit = 'GC'
                c_id = 1
                c_balance = 0
                c_ytd_payment = 0
                c_payment_cnt = 0
        else:
            try:
                result = select(client=self._client,
                                table=[CUSTOMER],
                                col=[C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
                                        C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
                                        C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT],
                                where=[(C_ID, eq, c_query),
                                        (C_W_ID, eq, c_w_id),
                                        (C_D_ID, eq, c_d_id)])
                if result is None:
                    return SQLState.ABORT
                result = result[0]
                c_id, c_first, c_midele, c_last, \
                    c_street_1, c_street_2, c_city, c_state, \
                    c_zip, c_phone, c_since, \
                    c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt = result  # result[len(result)//2]
                c_id = eval(c_id)
                c_balance = eval(c_balance)
                c_ytd_payment = eval(c_ytd_payment)
                c_payment_cnt = eval(c_payment_cnt)
            except Exception as e:
                c_credit = 'GC'
                c_id = 1
                c_balance = 0
                c_ytd_payment = 0
                c_payment_cnt = 0
        # 更新客户余额和付款信息
        if update(client=self._client,
                    table=CUSTOMER,
                    set=[(C_BALANCE, c_balance + h_amount),
                        (C_YTD_PAYMENT, c_ytd_payment + 1),
                        (C_PAYMENT_CNT, c_payment_cnt + 1)],
                    where=[(C_W_ID, eq, w_id), (C_D_ID, eq, d_id), (C_ID, eq, c_id)]) == SQLState.ABORT:
            return SQLState.ABORT
        
        if c_credit == 'BC':
            c_data = ''
            try:
                res = select(client=self._client,
                            table=[CUSTOMER],
                            col=[C_DATA],
                            where=[(C_ID, eq, c_id),
                                    (C_W_ID, eq, c_w_id),
                                    (C_D_ID, eq, c_d_id)])
                if res is None:
                    log_error("Error: select from CUSTOMER for C_DATA failed or empty")
                    error("Error: select from CUSTOMER for C_DATA failed or empty")
                    exit(1)
                    return SQLState.ABORT
                #! 因为当前建表长度限制是50
                c_data = (''.join(map(str, [c_id, c_d_id, c_w_id, d_id, h_amount]))
                            + res[0][0])[0:50]
            except Exception as e:
                print(f'error {CUSTOMER}: {str(e)}')
                pass

            if update(client=self._client,
                    table=CUSTOMER,
                    set=[(C_DATA, "'" + c_data + "'")],
                    where=[(C_ID, eq, c_id), (C_W_ID, eq, c_w_id), (C_D_ID, eq, c_d_id)]) == SQLState.ABORT:
                return SQLState.ABORT

        payment_time = "'" + current_time() + "'"
        h_data = "'" + w_name + '    ' + d_name + "'"
        if insert(client=self._client,
                  table=HISTORY,
                  rows=[c_id, c_d_id, c_w_id, d_id, w_id, payment_time, h_amount, h_data]) == SQLState.ABORT:
            return SQLState.ABORT

        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Payment')
        return SQLState.SUCCESS

    def do_order_status(self, w_id:int, d_id:int, c_query:Union[str, int]) -> SQLState:
        '''
        功能: 处理订单状态查询 主要步骤:
        根据客户ID或姓名查找客户
        查找客户最近的订单
        查找订单行项目
        '''
        c_id = 0
        o_id = 0
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Order Status')
        if type(c_query) is str:
            c_query = "'" + c_query + "'"
            try:
                res = select(client=self._client,
                                table=[CUSTOMER],
                                col=[C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE],
                                where=[(C_LAST, eq, c_query),
                                        (C_W_ID, eq, w_id),
                                        (C_D_ID, eq, d_id)],
                                # order_by=C_FIRST,
                                # asc=True
                                )
                if res is None:
                    log_error("Error: select from CUSTOMER in do_order_status failed or empty")
                    error("Error: select from CUSTOMER in do_order_status failed or empty")
                    exit(1)
                    return SQLState.ABORT
                c_id, c_first, c_middle, c_last, c_balance = res[0]
                c_id = eval(c_id)
                c_balance = eval(c_balance)
            except Exception as e:
                print(f'error {CUSTOMER}: {str(e)}')
                pass
        else:
            try:
                res = select(client=self._client,
                                table=[CUSTOMER],
                                col=[C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE],
                                where=[(C_ID, eq, c_query),
                                        (C_W_ID, eq, w_id),
                                        (C_D_ID, eq, d_id)])
                if res is None:
                    log_error("Error: select from CUSTOMER by C_ID in do_order_status failed or empty")
                    error("Error: select from CUSTOMER by C_ID in do_order_status failed or empty")
                    exit(1)
                    return SQLState.ABORT
                c_id, c_first, c_middle, c_last, c_balance = res[0]
                c_id = eval(c_id)
                c_balance = eval(c_balance)
            except Exception as e:
                print(f'error {CUSTOMER}: {str(e)}')
                pass

        try:
            res = select(client=self._client,
                            table=[ORDERS],
                            col=[O_ID, O_ENTRY_D, O_CARRIER_ID],
                            where=[(O_W_ID, eq, w_id),
                                (O_D_ID, eq, d_id),
                                (O_C_ID, eq, c_id)],
                            order_by=O_ID,
                            asc=False)
            if res is None:
                log_error("Error: select from ORDERS in do_order_status failed or empty")
                error("Error: select from ORDERS in do_order_status failed or empty")
                exit(1)
                return SQLState.ABORT
            o_id, o_entry_d, o_carrier_id = res[0]
            o_id = eval(o_id)
        except Exception as e:
            print(f'error {ORDERS}: {str(e)}')
            pass

        try:
            res = select(client=self._client,
                            table=[ORDER_LINE],
                            col=[OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D],
                            where=[(OL_W_ID, eq, w_id),
                                (OL_D_ID, eq, d_id),
                                (OL_O_ID, eq, o_id)])
            if res is None:
                log_error("Error: select from ORDER_LINE in do_order_status failed or empty")
                error("Error: select from ORDER_LINE in do_order_status failed or empty")
                exit(1)
                return SQLState.ABORT
            # print(res)
        except Exception as e:
            print(f'error {ORDER_LINE}: {str(e)}')
            pass

        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Order Status')
        return SQLState.SUCCESS

    def do_delivery(self, w_id:int, o_carrier_id:int) -> SQLState:
        '''
        功能: 处理订单交付 主要步骤:
        查找最早未交付的订单
        更新订单交付承运人ID (ORDERS.O_CARRIER_ID)
        更新订单行交付日期 (ORDER_LINE.OL_DELIVERY_D)
        更新客户余额和交付计数 (CUSTOMER)
        删除新订单记录 (NEW_ORDERS)
        '''
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Delivery')
        for d_id in range(1, config.D_ID_MAX):
            try:
                res = select(client=self._client,
                                table=[NEW_ORDERS],
                                col=[NO_O_ID],
                                where=[(NO_W_ID, eq, w_id),
                                    (NO_D_ID, eq, d_id)],
                                order_by=NO_O_ID,
                                asc=True)
                if res is None:
                    log_error("Error: select from NEW_ORDERS in do_delivery failed or empty")
                    error("Error: select from NEW_ORDERS in do_delivery failed or empty")
                    exit(1)
                    return SQLState.ABORT
                no_o_id = eval(res[0][0])
            except Exception as e:
                # print(f'error {NEW_ORDERS}: {str(e)}')
                continue

            try:
                res = select(client=self._client,
                                table=[ORDERS],
                                col=[O_C_ID],
                                where=[(O_W_ID, eq, w_id),
                                    (O_D_ID, eq, d_id),
                                    (O_ID, eq, no_o_id)])
                if res is None:
                    log_error("Error: select from ORDERS in do_delivery failed or empty")
                    error("Error: select from ORDERS in do_delivery failed or empty")
                    exit(1)
                    return SQLState.ABORT
                o_c_id = eval(res[0][0])
            except Exception as e:
                print(f'error {ORDERS}: {str(e)}')
                continue

            if update(client=self._client,
                        table=ORDERS,
                        set=[(O_CARRIER_ID, o_carrier_id)],
                        where=[(O_W_ID, eq, w_id),
                                (O_D_ID, eq, d_id),
                                (O_ID, eq, no_o_id)]) == SQLState.ABORT:
                return SQLState.ABORT

            delivery_time = "'" + current_time() + "'"
            if update(client=self._client,
                        table=ORDER_LINE,
                        set=[(OL_DELIVERY_D, delivery_time)],
                        where=[(OL_W_ID, eq, w_id),
                                (OL_D_ID, eq, d_id),
                                (OL_O_ID, eq, no_o_id)]) == SQLState.ABORT:
                return SQLState.ABORT

            try:
                res = select(client=self._client,
                                table=[ORDER_LINE],
                                col=[SUM(OL_AMOUNT)],
                                where=[(OL_W_ID, eq, w_id),
                                    (OL_D_ID, eq, d_id),
                                    (OL_O_ID, eq, no_o_id)])
                if res is None:
                    log_error("Error: select from ORDER_LINE in do_delivery failed or empty")
                    error("Error: select from ORDER_LINE in do_delivery failed or empty")
                    exit(1)
                    return SQLState.ABORT
                ol_total = eval(res[0][0])
            except Exception as e:
                print(f'error {ORDER_LINE}: {str(e)}')
                continue

            try:
                res = select(client=self._client,
                                table=[CUSTOMER],
                                col=[C_BALANCE, C_DELIVERY_CNT],
                                where=[(C_W_ID, eq, w_id),
                                    (C_D_ID, eq, d_id),
                                    (C_ID, eq, o_c_id)])
                if res is None:
                    log_error("Error: select from CUSTOMER in do_delivery failed or empty")
                    error("Error: select from CUSTOMER in do_delivery failed or empty")
                    exit(1)
                    return SQLState.ABORT
                c_balance, c_delivery_cnt = res[0]
                c_balance = eval(c_balance)
                c_delivery_cnt = eval(c_delivery_cnt)
            except Exception as e:
                print(f'error {CUSTOMER}: {str(e)}')
                continue

            if update(client=self._client,
                        table=CUSTOMER,
                        set=[(C_BALANCE, c_balance + ol_total),
                            (C_DELIVERY_CNT, c_delivery_cnt + 1)],
                        where=[(C_W_ID, eq, w_id),
                                (C_D_ID, eq, d_id),
                                (C_ID, eq, o_c_id)]) == SQLState.ABORT:
                return SQLState.ABORT

            if delete(client=self._client,
                        table=NEW_ORDERS,
                        where=[(NO_W_ID, eq, w_id),
                                (NO_D_ID, eq, d_id),
                                (NO_O_ID, eq, no_o_id)]) == SQLState.ABORT:
                return SQLState.ABORT
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Delivery')
        return SQLState.SUCCESS

    def do_stock_level(self, w_id:int, d_id:int, level:int) -> SQLState:
        '''
        功能: 处理库存水平查询 主要步骤:
        获取区域的下一个订单ID
        查找最近20个订单的订单行项目
        统计库存低于阈值的商品数量
        '''
        if self._client.send_cmd("BEGIN;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('+ Stock Level')
        d_next_o_id = 0
        ol_i_ids = []
        try:
            res = select(client=self._client,
                            table=[DISTRICT],
                            col=[D_NEXT_O_ID],
                            where=[(D_W_ID, eq, w_id),
                                (D_ID, eq, d_id)])
            if res is None:
                log_error("Error: select from DISTRICT in do_stock_level failed or empty")
                error("Error: select from DISTRICT in do_stock_level failed or empty")
                exit(1)
                return SQLState.ABORT
            d_next_o_id = eval(res[0][0])
        except Exception as e:
            print(f'error {DISTRICT}: {str(e)}')
            pass

        try:
            res = select(client=self._client,
                            table=[ORDER_LINE],
                            col=[OL_I_ID],
                            where=[(OL_W_ID, eq, w_id),
                                (OL_D_ID, eq, d_id),
                                (OL_O_ID, beq, d_next_o_id - 20),
                                (OL_O_ID, lt, d_next_o_id)])
            if res is None:
                log_error("Error: select from ORDER_LINE in do_stock_level failed or empty")
                error("Error: select from ORDER_LINE in do_stock_level failed or empty")
                exit(1)
                return SQLState.ABORT
            ol_i_ids = [eval(r[0]) for r in res]
        except Exception as e:
            print(f'error {ORDER_LINE}: {str(e)}')
            pass

        low_stock_count = 0
        try:

            #! 因为暂时不支持in 语法
            for ol_i_id in ol_i_ids:
                res = select(client=self._client,
                                table=[STOCK],
                                col=[S_QUANTITY],
                                where=[(S_W_ID, eq, w_id),
                                    (S_I_ID, eq, ol_i_id)])
                if res is None:
                    log_error("Error: select from STOCK in do_stock_level failed or empty")
                    error("Error: select from STOCK in do_stock_level failed or empty")
                    exit(1)
                    return SQLState.ABORT
                s_quantity = eval(res[0][0])
                if s_quantity < level:
                    low_stock_count += 1
        except Exception as e:
            print(f'error {STOCK}: {str(e)}')
            pass
        if self._client.send_cmd("COMMIT;") == SQLState.ABORT:
            return SQLState.ABORT
        # print('- Stock Level')
        return SQLState.SUCCESS
