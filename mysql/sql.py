from typing import Optional, Union, Tuple, List
from enum import Enum  # 引入枚举类型

from db.table_layouts import num_of_cols
from debug_utils import TRACE_FUNCTION

Value = Union[int, float, str]
WhereCondition = Tuple[str, str, Value]
WhereConditions = List[WhereCondition]
SetCondition = Tuple[str, Value]
SetConditions = List[SetCondition]

# Operator
ALL = '*'
SELECT = 'select'
FROM = 'from'
WHERE = 'where'
AND = ' and '
ORDER_BY = 'order by'
DESC = 'desc'
ASC = 'asc'
INSERT = 'insert'
VALUES = 'values'
UPDATE = 'update'
DELETE = 'delete'
SET = 'set'
eq = '='
bt = '>'
lt = '<'
beq = '>='
leq = '<='


class SQLState(Enum):
    SUCCESS = 7,
    ABORT = 3

# 生成where条件的设置
def gen(ele:WhereCondition) -> str:
    return str(ele[0]) + ' ' + str(ele[1]) + ' %s'

# 暂时只支持单条件排序
def select(
    client, 
    table:List[str],
    col:List[str] = [ALL],
    where:Optional[WhereConditions] = None,
    order_by:Optional[str] = None, 
    asc:bool = False
) -> Union[List[List[str]], None]:
    with TRACE_FUNCTION():
        param = []
        if where :
            param = [ele[-1] for ele in where]

        table_str = ','.join(table)
        where_str = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        order_by_str = ' '.join([ORDER_BY, order_by, ASC if asc else DESC]) if order_by else ''
        sql = ' '.join([SELECT, ','.join(col), FROM, table_str, where_str, order_by_str, ';'])
        for i in param:
            sql = sql.replace("%s", str(i), 1)

        result = client.send_cmd(sql)

        if result is None or result == '' or result.startswith('Error') or result.startswith('abort'):
            return None

        # 使用字符串分割提取数字部分
        ## 1. 确定要获取多少列属性
        real_col_num = 0
        if (len(col) == 1 and col[0] == '*'):
            for i in table:
                real_col_num = real_col_num + num_of_cols[i]
        else:
            real_col_num = len(col)
        '''
        +------------------+
        |               id |
        +------------------+
        |                1 |
        |                2 |
        +------------------+
        '''
        ## 2. 跳过表头部分
        # 找三次 '\n' 跳过前3行：顶部边框、列名行、表头下方边框
        shuxian_idx = result.find('\n')  # 第1个换行符
        shuxian_idx = result.find('\n', shuxian_idx + 1)  # 第2个换行符
        shuxian_idx = result.find('\n', shuxian_idx + 1)  # 第3个换行符

        if(result.find('|', shuxian_idx + 1) == -1):
            return None

        # 现在 shuxian_idx 指向第3行末尾，数据行从下一行开始
        ## 3. 初始化结果集
        results_allline = []
        result_oneline = []
        ## 4. 双层循环提取value
        while (True):
            for i in range(real_col_num):
                start_index = result.find('|', shuxian_idx + 1) + 1
                end_index = result.find('|', start_index)
                if (end_index == -1):
                    # print(results_allline)
                    return results_allline
                extracted_value = result[start_index:end_index].strip()
                result_oneline.append(extracted_value)
                shuxian_idx = start_index
            results_allline.append(result_oneline)
            result_oneline = []
            start_index = result.find('|', shuxian_idx + 1) + 1
            end_index = result.find('|', start_index)
            if (end_index == -1):
                return results_allline
            shuxian_idx = start_index



def insert(client, table:str , rows:List[Value]) -> SQLState:
    with TRACE_FUNCTION():
        values = ''.join([VALUES, '(', ','.join(['%s' for i in range(num_of_cols[table])]), ')'])
        sql = ' '.join([INSERT, "into", table, values, ';'])
        for i in rows:
            sql = sql.replace("%s", str(i), 1)

        if client.send_cmd(sql).startswith('abort'):
            return SQLState.ABORT
        else :
            return SQLState.SUCCESS

def update(client, table:str, set:SetConditions, where:WhereConditions) -> SQLState:
    with TRACE_FUNCTION():
        param = [ele[-1] for ele in where]
        
        where_str = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        var = [e[0] + ' = %s' for e in set]
        val = [e[1] for e in set]

        sql = ' '.join([UPDATE, table, SET, ','.join(var), where_str, ';'])
        # print(sql,val+param)
        for i in val:
            sql = sql.replace("%s", str(i), 1)
        for i in param:
            sql = sql.replace("%s", str(i), 1)
        # print(sql)
        if client.send_cmd(sql).startswith('abort'):
            return SQLState.ABORT
        else :
            return SQLState.SUCCESS


def delete(client, table:str, where:Optional[WhereConditions] = None) -> SQLState:
    with TRACE_FUNCTION():
        param = []
        if where:
            param = [ele[-1] for ele in where]
        where_str = ' '.join([WHERE, AND.join([gen(ele) for ele in where])]) if where else ''
        sql = ' '.join([DELETE, FROM, table, where_str, ';'])
        for i in param:
            sql = sql.replace("%s", str(i), 1)
        if client.send_cmd(sql).startswith('abort'):
            return SQLState.ABORT
        else :
            return SQLState.SUCCESS
