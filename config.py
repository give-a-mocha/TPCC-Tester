# TPC-C 测试配置文件

class Config:
    def __init__(self, cnt_w=1):
        # 基础配置
        self.CNT_W = cnt_w  # 仓库数量
        self.CNT_ITEM = 100000  # 商品数量（固定）
        
        # 根据仓库数量计算的配置
        self.CNT_STOCK = self.CNT_W * 100000
        self.CNT_DISTRICT = self.CNT_W * 10
        self.CNT_CUSTOMER = self.CNT_W * 10 * 3000
        self.CNT_HISTORY = self.CNT_W * 10 * 3000
        self.CNT_ORDERS = self.CNT_W * 10 * 3000
        self.CNT_NEW_ORDERS = self.CNT_W * 10 * 900
        self.CNT_ORDER_LINE = self.CNT_ORDERS * 10
        
        # 最大值配置
        self.W_ID_MAX = self.CNT_W + 1
        self.D_ID_MAX = 11
    
    def get_tables_info(self):
        """获取表信息列表"""
        from mysql.sql import (WAREHOUSE, DISTRICT, CUSTOMER, HISTORY, 
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
    config = Config(cnt_w)
    return config
