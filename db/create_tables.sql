-- TPC-C 基准测试数据库表结构

-- 仓库表：存储仓库信息
CREATE TABLE warehouse
(
    w_id       int,        -- 仓库ID（主键）
    w_name     char(10),   -- 仓库名称
    w_street_1 char(20),   -- 仓库地址第一行
    w_street_2 char(20),   -- 仓库地址第二行
    w_city     char(20),   -- 仓库所在城市
    w_state    char(2),    -- 仓库所在州/省
    w_zip      char(9),    -- 仓库邮政编码
    w_tax      float,      -- 仓库税率
    w_ytd      float       -- 年初至今销售额
);

-- 商品表：存储商品基本信息
CREATE TABLE item
(
    i_id    int,        -- 商品ID（主键）
    i_im_id int,        -- 商品图片ID
    i_name  char(24),   -- 商品名称
    i_price float,      -- 商品价格
    i_data  char(50)    -- 商品描述数据
);

-- 库存表：存储各仓库的商品库存信息
CREATE TABLE stock
(
    s_i_id       int,        -- 商品ID（外键，引用item.i_id）
    s_w_id       int,        -- 仓库ID（外键，引用warehouse.w_id）
    s_quantity   int,        -- 库存数量
    s_dist_01    char(24),   -- 区域1配送信息
    s_dist_02    char(24),   -- 区域2配送信息
    s_dist_03    char(24),   -- 区域3配送信息
    s_dist_04    char(24),   -- 区域4配送信息
    s_dist_05    char(24),   -- 区域5配送信息
    s_dist_06    char(24),   -- 区域6配送信息
    s_dist_07    char(24),   -- 区域7配送信息
    s_dist_08    char(24),   -- 区域8配送信息
    s_dist_09    char(24),   -- 区域9配送信息
    s_dist_10    char(24),   -- 区域10配送信息
    s_ytd        int,        -- 年初至今销售数量
    s_order_cnt  int,        -- 订单数量统计
    s_remote_cnt int,        -- 远程订单数量统计
    s_data       char(50)    -- 库存描述数据
);

-- 销售区域表：存储各仓库下的销售区域信息
CREATE TABLE district
(
    d_id        int,        -- 区域ID（主键）
    d_w_id      int,        -- 仓库ID（外键，引用warehouse.w_id）
    d_name      char(10),   -- 区域名称
    d_street_1  char(20),   -- 区域地址第一行
    d_street_2  char(20),   -- 区域地址第二行
    d_city      char(20),   -- 区域所在城市
    d_state     char(2),    -- 区域所在州/省
    d_zip       char(9),    -- 区域邮政编码
    d_tax       float,      -- 区域税率
    d_ytd       float,      -- 年初至今销售额
    d_next_o_id int         -- 下一个订单ID
);

-- 客户表：存储客户信息
CREATE TABLE customer
(
    c_id           int,          -- 客户ID（主键）
    c_d_id         int,          -- 区域ID（外键，引用district.d_id）
    c_w_id         int,          -- 仓库ID（外键，引用warehouse.w_id）
    c_first        char(16),     -- 客户名
    c_middle       char(2),      -- 客户中间名
    c_last         char(16),     -- 客户姓
    c_street_1     char(20),     -- 客户地址第一行
    c_street_2     char(20),     -- 客户地址第二行
    c_city         char(20),     -- 客户所在城市
    c_state        char(2),      -- 客户所在州/省
    c_zip          char(9),      -- 客户邮政编码
    c_phone        char(16),     -- 客户电话号码
    c_since        datetime,     -- 客户注册时间
    c_credit       char(2),      -- 客户信用等级（GC=良好信用，BC=不良信用）
    c_credit_lim   float,        -- 客户信用额度
    c_discount     float,        -- 客户折扣率
    c_balance      float,        -- 客户账户余额
    c_ytd_payment  float,        -- 年初至今支付金额
    c_payment_cnt  int,          -- 支付次数统计
    c_delivery_cnt int,          -- 配送次数统计
    c_data         char(300)     -- 客户其他数据
);

-- 历史交易表：存储客户支付历史记录
CREATE TABLE history
(
    h_c_id     int,          -- 客户ID（外键，引用customer.c_id）
    h_c_d_id   int,          -- 客户区域ID（外键，引用district.d_id）
    h_c_w_id   int,          -- 客户仓库ID（外键，引用warehouse.w_id）
    h_d_id     int,          -- 交易区域ID（外键，引用district.d_id）
    h_w_id     int,          -- 交易仓库ID（外键，引用warehouse.w_id）
    h_datetime datetime,     -- 交易时间
    h_amount   float,        -- 交易金额
    h_data     char(24)      -- 交易描述数据
);

-- 订单表：存储订单信息
CREATE TABLE orders
(
    o_id         int,          -- 订单ID（主键）
    o_d_id       int,          -- 区域ID（外键，引用district.d_id）
    o_w_id       int,          -- 仓库ID（外键，引用warehouse.w_id）
    o_c_id       int,          -- 客户ID（外键，引用customer.c_id）
    o_entry_d    datetime,     -- 订单创建时间
    o_carrier_id int,          -- 承运商ID（NULL表示未配送）
    o_ol_cnt     int,          -- 订单行数（商品种类数）
    o_all_local  int           -- 是否为本地订单（1=本地，0=远程）
);

-- 新订单表：存储待处理的新订单
CREATE TABLE new_orders
(
    no_o_id int,              -- 订单ID（外键，引用orders.o_id）
    no_d_id int,              -- 区域ID（外键，引用district.d_id）
    no_w_id int               -- 仓库ID（外键，引用warehouse.w_id）
);

-- 订单明细表：存储订单中的具体商品信息
CREATE TABLE order_line
(
    ol_o_id        int,        -- 订单ID（外键，引用orders.o_id）
    ol_d_id        int,        -- 区域ID（外键，引用district.d_id）
    ol_w_id        int,        -- 仓库ID（外键，引用warehouse.w_id）
    ol_number      int,        -- 订单行号（在单个订单内的序号）
    ol_i_id        int,        -- 商品ID（外键，引用item.i_id）
    ol_supply_w_id int,        -- 供应仓库ID（外键，引用warehouse.w_id）
    ol_delivery_d  datetime,   -- 配送时间（NULL表示未配送）
    ol_quantity    int,        -- 商品数量
    ol_amount      float,      -- 订单行金额
    ol_dist_info   char(24)    -- 配送信息
);
