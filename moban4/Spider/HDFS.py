import os
import sys
import subprocess
import json
import csv
import datetime
import time
import tempfile
import findspark
import logging
import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("HDFS-Export")

def create_spark_session():
    """创建SparkSession"""
    # 设置Hadoop环境变量
    os.environ['HADOOP_HOME'] = r"C:\Program Files\Hadoop"
    os.environ['PATH'] = os.environ['PATH'] + r";C:\Program Files\Hadoop\bin"
    
    # 初始化findspark
    findspark.init()
    
    return SparkSession.builder \
        .appName("NewRetailHDFSExport") \
        .config("spark.jars", "mysql-connector-java-5.1.49.jar") \
        .config("spark.driver.extraClassPath", "mysql-connector-java-5.1.49.jar") \
        .config("spark.executor.extraClassPath", "mysql-connector-java-5.1.49.jar") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def connect_db():
    """连接数据库"""
    return pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='root',
        db='new_retail'
    )

def get_table_schema(table_name):
    """获取表结构"""
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        # 获取表字段信息
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        # 创建Spark schema
        fields = []
        for col in columns:
            field_name = col[0]
            field_type = col[1].lower()
            
            if 'int' in field_type:
                spark_type = IntegerType()
            elif 'decimal' in field_type or 'numeric' in field_type:
                spark_type = DecimalType(10, 2)
            elif 'float' in field_type or 'double' in field_type:
                spark_type = DoubleType()
            elif 'datetime' in field_type or 'timestamp' in field_type:
                spark_type = TimestampType()
            elif 'date' in field_type:
                spark_type = DateType()
            elif 'char' in field_type or 'text' in field_type:
                spark_type = StringType()
            elif 'bool' in field_type or 'tinyint(1)' in field_type:
                spark_type = BooleanType()
            else:
                spark_type = StringType()
                
            fields.append(StructField(field_name, spark_type, True))
        
        return StructType(fields)
    
    except Exception as e:
        logger.error(f"获取表结构时发生错误: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def export_table_to_hdfs(spark, table_name, hdfs_path, batch_size=10000):
    """将表数据导出到HDFS"""
    logger.info(f"开始导出表 {table_name} 到HDFS路径 {hdfs_path}")
    
    # JDBC连接参数
    jdbc_url = "jdbc:mysql://localhost:3306/new_retail?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.jdbc.Driver"
    }
    
    try:
        # 从MySQL读取数据
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )
        
        # 保存到HDFS
        df.write.mode("overwrite").parquet(f"{hdfs_path}/{table_name}")
        
        logger.info(f"表 {table_name} 导出成功，共 {df.count()} 条记录")
        return True
    
    except Exception as e:
        logger.error(f"导出表 {table_name} 时发生错误: {e}")
        return False

def export_all_tables_to_hdfs(base_hdfs_path="/user/new_retail/data"):
    """导出所有表到HDFS"""
    logger.info("开始导出所有表数据到HDFS")
    
    try:
        # 创建Spark会话
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession创建成功！")
        
        # 获取所有表
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # 导出每个表
        for table in tables:
            export_table_to_hdfs(spark, table, base_hdfs_path)
        
        logger.info("所有表导出完成！")
        
    except Exception as e:
        logger.error(f"导出过程中发生错误: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("SparkSession已关闭")

def export_table_incremental(spark, table_name, hdfs_path, timestamp_column="update_time", last_timestamp=None):
    """增量导出表数据到HDFS"""
    logger.info(f"开始增量导出表 {table_name} 到HDFS路径 {hdfs_path}")
    
    # JDBC连接参数
    jdbc_url = "jdbc:mysql://localhost:3306/new_retail?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.jdbc.Driver"
    }
    
    try:
        # 构建查询条件
        query = f"SELECT * FROM {table_name}"
        if last_timestamp and timestamp_column:
            query += f" WHERE {timestamp_column} > '{last_timestamp}'"
        
        # 从MySQL读取增量数据
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) as temp",
            properties=connection_properties
        )
        
        if df.count() > 0:
            # 确定保存模式
            save_mode = "append" if last_timestamp else "overwrite"
            
            # 保存到HDFS
            df.write.mode(save_mode).parquet(f"{hdfs_path}/{table_name}")
            
            logger.info(f"表 {table_name} 增量导出成功，共 {df.count()} 条记录")
            
            # 返回最新的时间戳作为下次增量导出的基准
            if timestamp_column:
                max_timestamp = df.agg({timestamp_column: "max"}).collect()[0][0]
                return max_timestamp
        else:
            logger.info(f"表 {table_name} 没有新数据需要导出")
            
        return last_timestamp
    
    except Exception as e:
        logger.error(f"增量导出表 {table_name} 时发生错误: {e}")
        return last_timestamp

def check_hdfs_path(hdfs_path):
    """检查HDFS路径是否存在，不存在则创建"""
    try:
        result = subprocess.run(
            ["hadoop", "fs", "-test", "-e", hdfs_path],
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        
        # 如果路径不存在（返回值非0）
        if result.returncode != 0:
            logger.info(f"HDFS路径 {hdfs_path} 不存在，尝试创建...")
            subprocess.run(
                ["hadoop", "fs", "-mkdir", "-p", hdfs_path],
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            logger.info(f"HDFS路径 {hdfs_path} 创建成功")
        
        return True
    except Exception as e:
        logger.error(f"检查HDFS路径时发生错误: {e}")
        return False

def get_last_sync_timestamps():
    """获取上次同步的时间戳记录"""
    timestamp_file = "hdfs_sync_timestamps.json"
    
    if os.path.exists(timestamp_file):
        try:
            with open(timestamp_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    else:
        return {}

def save_sync_timestamps(timestamps):
    """保存同步时间戳记录"""
    timestamp_file = "hdfs_sync_timestamps.json"
    
    try:
        with open(timestamp_file, 'w') as f:
            json.dump(timestamps, f)
    except Exception as e:
        logger.error(f"保存同步时间戳记录时发生错误: {e}")

def synchronize_data_to_hdfs(base_hdfs_path="/user/new_retail/data", incremental=True):
    """同步数据到HDFS，支持增量同步"""
    logger.info(f"开始{'增量' if incremental else '全量'}同步数据到HDFS...")
    
    try:
        # 创建Spark会话
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession创建成功！")
        
        # 检查并创建HDFS基础路径
        check_hdfs_path(base_hdfs_path)
        
        # 获取所有表
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        # 获取上次同步时间戳
        timestamps = get_last_sync_timestamps() if incremental else {}
        
        # 同步每个表
        for table in tables:
            logger.info(f"处理表 {table}...")
            
            # 检查表是否有更新时间字段
            timestamp_column = "update_time"
            conn = connect_db()
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {table} LIKE 'update_time'")
            has_update_time = cursor.fetchone() is not None
            if not has_update_time:
                cursor.execute(f"SHOW COLUMNS FROM {table} LIKE 'create_time'")
                has_create_time = cursor.fetchone() is not None
                timestamp_column = "create_time" if has_create_time else None
            cursor.close()
            conn.close()
            
            # 增量同步
            if incremental and timestamp_column:
                last_timestamp = timestamps.get(table)
                new_timestamp = export_table_incremental(
                    spark, table, base_hdfs_path, 
                    timestamp_column, last_timestamp
                )
                if new_timestamp:
                    timestamps[table] = str(new_timestamp)
            # 全量同步
            else:
                export_table_to_hdfs(spark, table, base_hdfs_path)
        
        # 保存同步时间戳
        if incremental:
            save_sync_timestamps(timestamps)
        
        logger.info("同步完成！")
        
    except Exception as e:
        logger.error(f"同步过程中发生错误: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("SparkSession已关闭")

def hook_data_create():
    """钩子函数，可在DataCreate.py调用完成后执行同步"""
    logger.info("DataCreate.py执行完成，开始同步数据到HDFS...")
    synchronize_data_to_hdfs(incremental=False)  # 执行全量同步

def main():
    """主函数"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            # 全量同步
            synchronize_data_to_hdfs(incremental=False)
        elif sys.argv[1] == "--hook":
            # 钩子模式，用于DataCreate.py调用
            hook_data_create()
        else:
            # 默认增量同步
            synchronize_data_to_hdfs(incremental=True)
    else:
        # 默认增量同步
        synchronize_data_to_hdfs(incremental=True)

if __name__ == "__main__":
    main()
