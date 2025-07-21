# TPC-C 测试配置文件

class Config:
    CUST_PER_DIST = 3000
    DIST_PER_WARE = 10
    ORD_PER_DIST = 3000
    STOCK_PER_WARE = 100000
    ORDER_LINE_PER_ORDER = 10
    NEW_ORDER_PER_DIST = 900  # 每个地区新订单数量
    def __init__(self, cnt_w=1):
        # 基础配置
        self.CNT_W = cnt_w  # 仓库数量
        self.CNT_ITEM = 100000  # 商品数量（固定）
        
        # 根据仓库数量计算的配置
        self.CNT_STOCK = self.CNT_W * self.STOCK_PER_WARE
        self.CNT_DISTRICT = self.CNT_W * self.DIST_PER_WARE
        self.CNT_CUSTOMER = self.CNT_W * self.DIST_PER_WARE * self.CUST_PER_DIST
        self.CNT_HISTORY = self.CNT_CUSTOMER
        self.CNT_ORDERS = self.CNT_CUSTOMER
        # 避免float 运算
        self.CNT_NEW_ORDERS = self.CNT_W * self.DIST_PER_WARE * 900
        self.CNT_ORDER_LINE = self.CNT_ORDERS * self.ORDER_LINE_PER_ORDER
        
        # 最大值配置
        self.W_ID_MAX = self.CNT_W + 1
        self.D_ID_MAX = 11
    
    def get_tables_info(self):
        """获取表信息列表"""
        from db.table_layouts import (WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, 
                              NEW_ORDERS, ORDERS, ORDER_LINE, ITEM, STOCK)
        
        return [
            (WAREHOUSE, 'count_warehouse', self.CNT_W, 'count_warehouse'),
            (DISTRICT, 'count_district', self.CNT_DISTRICT, 'count_district'),
            (CUSTOMER, 'count_customer', self.CNT_CUSTOMER, 'count_customer'),
            (HISTORY, 'count_history', self.CNT_HISTORY, 'count_history'),
            (NEW_ORDERS, 'count_new_orders', self.CNT_NEW_ORDERS, 'count_new_orders'),
            (ORDERS, 'count_orders', self.CNT_ORDERS, 'count_orders'),
            (ORDER_LINE, 'count_order_line', self.CNT_ORDER_LINE, 'count_order_line'),
            (ITEM, 'count_item', self.CNT_ITEM, 'count_item'),
            (STOCK, 'count_stock', self.CNT_STOCK, 'count_stock')
        ]

# 全局配置实例
config = Config()

def set_warehouse_count(cnt_w):
    """设置仓库数量并更新全局配置"""
    global config
    # 直接修改现有 config 实例的属性，而不是创建一个新实例
    config.CNT_W = cnt_w
    config.CNT_STOCK = config.CNT_W * config.STOCK_PER_WARE
    config.CNT_DISTRICT = config.CNT_W * config.DIST_PER_WARE
    config.CNT_CUSTOMER = config.CNT_W * config.DIST_PER_WARE * config.CUST_PER_DIST
    config.CNT_HISTORY = config.CNT_CUSTOMER
    config.CNT_ORDERS = config.CNT_CUSTOMER
    config.CNT_NEW_ORDERS = config.CNT_W * config.DIST_PER_WARE * 900
    config.CNT_ORDER_LINE = config.CNT_ORDERS * config.ORDER_LINE_PER_ORDER
    config.W_ID_MAX = config.CNT_W + 1
    return config
