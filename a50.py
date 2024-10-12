import oracledb
import pandas as pd
import os
import re  # 导入正则表达式模块

# 设置 Oracle Instant Client 路径
oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_11_2")

# 设置 DSN，使用新的数据库配置
dsn = oracledb.makedsn("127.0.0.1", 1521, service_name="service_name")

# Oracle 数据库连接配置
connection = oracledb.connect(user="root", password="ok123", dsn=dsn)

# 查询 ACCOUNTCHECKING 用户下所有表名
query = "SELECT table_name FROM all_tables WHERE owner = 'YOU_TABLE'"

# 执行查询并获取所有表名
cursor = connection.cursor()
cursor.execute(query)
tables = cursor.fetchall()

# 设置保存文件的目录
output_dir = r'C:\Users\bf\table50'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 定义最大记录数的限制为 500,000
max_records = 500000

# 导出超过 50 万记录的表
for table in tables:
    table_name = table[0]
    
    # 跳过以 _数字 结尾的表名
    if re.search(r'_\d+$', table_name):
        print(f"跳过表 {table_name}（以 _数字 结尾）")
        continue

    # 打印表名
    print(f"处理表: {table_name}")
	
    # 计算表的总记录数
    count_query = f"SELECT COUNT(*) FROM YOU_TABLE.{table_name}"
    cursor.execute(count_query)
    total_records = cursor.fetchone()[0]
    
    # 如果表的记录数小于 50 万，跳过导出
    if total_records < max_records:
        print(f"跳过表 {table_name}（总记录数 {total_records}，小于 50 万条）")
        continue
    # 如果表的记录数大于 1000 万，跳过导出
    if total_records > 10000000:
        print(f"跳过表 {table_name}（总记录数 {total_records}，大于 1000 万条）")
        continue
		
    print(f"正在导出表: {table_name}，总记录数: {total_records}")

    # 分批导出数据
    num_chunks = (total_records // max_records) + 1  # 计算需要多少个文件
    for chunk_idx in range(num_chunks):
        # 定义每次查询的偏移量和记录数
        offset = chunk_idx * max_records
        query = f"""
        SELECT * FROM (
            SELECT ROWNUM AS rn, t.* FROM YOU_TABLE.{table_name} t
            WHERE ROWNUM <= {offset + max_records}
        ) WHERE rn > {offset}
        """
        
        # 从数据库中读取批量数据
        df = pd.read_sql(query, connection)
        
        # 创建 Excel 文件路径，文件名根据表名和 chunk_idx 创建
        file_path = os.path.join(output_dir, f"{table_name}_{chunk_idx + 1}.xlsx")
        
        # 将数据写入 Excel 文件
        df.to_excel(file_path, index=False)
        print(f"已导出文件: {file_path}")

# 关闭数据库连接
cursor.close()
connection.close()

print(f"符合条件的数据已成功导出到 {output_dir} 目录下")

