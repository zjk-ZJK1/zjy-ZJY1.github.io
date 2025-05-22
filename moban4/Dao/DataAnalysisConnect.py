import pymysql
import pandas as pd
from datetime import datetime, timedelta
import traceback

class DataAnalysisDao:
    def __init__(self):
        self.conn = None
    
    def connect(self):
        """连接数据库"""
        try:
            self.conn = pymysql.connect(
                host='localhost',
                port=3306,
                user='root',
                passwd='root',
                db='new_retail',
                charset='utf8mb4',
                connect_timeout=10
            )
            return self.conn
        except Exception as e:
            print(f"数据库连接失败: {e}")
            traceback.print_exc()
            return None
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
    
    def execute_query(self, sql, params=None):
        """执行查询并安全处理结果"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            if not conn:
                print("数据库连接失败")
                return []
                
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
                
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"查询执行失败: {e}")
            traceback.print_exc()
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_sales_trend(self):
        """获取销售趋势数据
        
        返回最近30天的每日销售额和订单数
        """
        try:
            # 查询最近30天的销售数据
            sql = """
            SELECT 
                DATE(create_time) as order_date,
                COUNT(*) as order_count,
                SUM(payment_amount) as total_amount
            FROM 
                sales_order
            WHERE 
                create_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY 
                DATE(create_time)
            ORDER BY 
                order_date
            """
            
            results = self.execute_query(sql)
            
            dates = []
            orders = []
            amounts = []
            
            for row in results:
                dates.append(row[0].strftime('%m-%d'))
                orders.append(row[1])
                amounts.append(float(row[2]) if row[2] is not None else 0)
            
            # 如果没有数据，返回今天的空数据
            if not dates:
                today = datetime.now().date()
                return {
                    'dates': [today.strftime('%m-%d')],
                    'orders': [0],
                    'amounts': [0]
                }
            
            # 如果数据不足30天，用0填充
            today = datetime.now().date()
            for i in range(30):
                date = today - timedelta(days=29-i)
                date_str = date.strftime('%m-%d')
                if date_str not in dates:
                    dates.append(date_str)
                    orders.append(0)
                    amounts.append(0)
            
            # 按日期排序
            sorted_data = sorted(zip(dates, orders, amounts), key=lambda x: datetime.strptime(x[0], '%m-%d'))
            dates = [item[0] for item in sorted_data]
            orders = [item[1] for item in sorted_data]
            amounts = [item[2] for item in sorted_data]
            
            return {
                'dates': dates,
                'orders': orders,
                'amounts': amounts
            }
            
        except Exception as e:
            print(f"获取销售趋势数据出错: {e}")
            traceback.print_exc()
            # 返回空数据而不是抛出异常
            return {
                'dates': [datetime.now().strftime('%m-%d')],
                'orders': [0],
                'amounts': [0]
            }
    
    def get_user_distribution(self):
        """获取用户地域分布数据
        
        返回各省份的用户数量
        """
        try:
            # 查询各省份的用户数量
            sql = """
            SELECT 
                province,
                COUNT(DISTINCT user_id) as user_count
            FROM 
                user_address
            GROUP BY 
                province
            ORDER BY 
                user_count DESC
            """
            
            results = self.execute_query(sql)
            
            regions = []
            max_count = 1  # 默认最大值为1，避免除以0错误
            
            for row in results:
                province = row[0]
                count = row[1]
                
                # 处理省份名称，确保与地图匹配
                if province.endswith('省') or province.endswith('市') or province.endswith('区') or province.endswith('自治区'):
                    province = province.rstrip('省市区自治区')
                
                regions.append({
                    'name': province,
                    'value': count
                })
                
                if count > max_count:
                    max_count = count
            
            # 如果没有数据，返回默认数据
            if not regions:
                return {
                    'regions': [{'name': '北京', 'value': 0}],
                    'max': 1
                }
            
            return {
                'regions': regions,
                'max': max_count
            }
            
        except Exception as e:
            print(f"获取用户地域分布数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'regions': [{'name': '北京', 'value': 0}],
                'max': 1
            }
    
    def get_category_sales(self):
        """获取商品类别销售占比数据
        
        返回各商品类别的销售额占比
        """
        try:
            # 查询各商品类别的销售额
            sql = """
            SELECT 
                pc.category_name,
                IFNULL(SUM(sd.actual_price), 0) as total_sales
            FROM 
                product_category pc
            LEFT JOIN 
                product p ON pc.id = p.category_id
            LEFT JOIN 
                sales_detail sd ON p.id = sd.product_id
            GROUP BY 
                pc.category_name
            ORDER BY 
                total_sales DESC
            """
            
            results = self.execute_query(sql)
            
            categories = []
            data = []
            
            for row in results:
                category = row[0]
                sales = float(row[1]) if row[1] is not None else 0
                
                categories.append(category)
                data.append({
                    'name': category,
                    'value': sales
                })
            
            # 如果没有数据，返回默认数据
            if not categories:
                return {
                    'categories': ['暂无数据'],
                    'data': [{'name': '暂无数据', 'value': 1}]
                }
            
            return {
                'categories': categories,
                'data': data
            }
            
        except Exception as e:
            print(f"获取商品类别销售占比数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'categories': ['暂无数据'],
                'data': [{'name': '暂无数据', 'value': 1}]
            }
    
    def get_channel_analysis(self):
        """获取销售渠道分析数据
        
        返回各销售渠道的订单数和销售额
        """
        try:
            # 查询各销售渠道的订单数和销售额
            sql = """
            SELECT 
                sc.channel_name,
                COUNT(so.id) as order_count,
                IFNULL(SUM(so.payment_amount), 0) as total_amount
            FROM 
                sales_channel sc
            LEFT JOIN 
                sales_order so ON sc.id = so.channel_id
            GROUP BY 
                sc.channel_name
            ORDER BY 
                total_amount DESC
            """
            
            results = self.execute_query(sql)
            
            channels = []
            orders = []
            amounts = []
            
            for row in results:
                channels.append(row[0])
                orders.append(row[1])
                amounts.append(float(row[2]) if row[2] is not None else 0)
            
            # 如果没有数据，返回默认数据
            if not channels:
                return {
                    'channels': ['暂无数据'],
                    'orders': [0],
                    'amounts': [0]
                }
            
            return {
                'channels': channels,
                'orders': orders,
                'amounts': amounts
            }
            
        except Exception as e:
            print(f"获取销售渠道分析数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'channels': ['暂无数据'],
                'orders': [0],
                'amounts': [0]
            }
    
    def get_user_behavior(self):
        """获取用户行为分析数据
        
        返回各类用户行为的占比
        """
        try:
            # 查询各类用户行为的数量
            sql = """
            SELECT 
                behavior_type,
                COUNT(*) as behavior_count
            FROM 
                user_behavior
            GROUP BY 
                behavior_type
            ORDER BY 
                behavior_count DESC
            """
            
            results = self.execute_query(sql)
            
            data = []
            
            behavior_map = {
                'view': '浏览',
                'search': '搜索',
                'cart': '加购',
                'purchase': '购买'
            }
            
            for row in results:
                behavior = row[0]
                count = row[1]
                
                # 转换行为类型为中文
                behavior_name = behavior_map.get(behavior, behavior)
                
                data.append({
                    'name': behavior_name,
                    'value': count
                })
            
            # 如果没有数据，返回默认数据
            if not data:
                return {
                    'data': [
                        {'name': '暂无数据', 'value': 1}
                    ]
                }
            
            return {
                'data': data
            }
            
        except Exception as e:
            print(f"获取用户行为分析数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'data': [
                    {'name': '暂无数据', 'value': 1}
                ]
            }
    
    def get_payment_methods(self):
        """获取支付方式分布数据
        
        返回各支付方式的订单数占比
        """
        try:
            # 查询各支付方式的订单数
            sql = """
            SELECT 
                payment_method,
                COUNT(*) as order_count
            FROM 
                sales_order
            WHERE 
                payment_status = 1  -- 已支付的订单
            GROUP BY 
                payment_method
            ORDER BY 
                order_count DESC
            """
            
            results = self.execute_query(sql)
            
            methods = []
            data = []
            
            payment_map = {
                'alipay': '支付宝',
                'wechat': '微信支付',
                'card': '银行卡',
                'cash': '现金'
            }
            
            for row in results:
                method = row[0]
                count = row[1]
                
                # 转换支付方式为中文
                method_name = payment_map.get(method, method)
                
                methods.append(method_name)
                data.append({
                    'name': method_name,
                    'value': count
                })
            
            # 如果没有数据，返回默认数据
            if not methods:
                return {
                    'methods': ['暂无数据'],
                    'data': [{'name': '暂无数据', 'value': 1}]
                }
            
            return {
                'methods': methods,
                'data': data
            }
            
        except Exception as e:
            print(f"获取支付方式分布数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'methods': ['暂无数据'],
                'data': [{'name': '暂无数据', 'value': 1}]
            }
    
    def get_hot_products(self):
        """获取热销商品排行数据
        
        返回销量最高的10个商品
        """
        try:
            # 查询销量最高的10个商品
            sql = """
            SELECT 
                p.product_name,
                SUM(sd.quantity) as total_quantity
            FROM 
                sales_detail sd
            JOIN 
                product p ON sd.product_id = p.id
            GROUP BY 
                p.product_name
            ORDER BY 
                total_quantity DESC
            LIMIT 10
            """
            
            results = self.execute_query(sql)
            
            products = []
            sales = []
            
            for row in results:
                products.append(row[0])
                sales.append(row[1])
            
            # 如果没有数据，返回默认数据
            if not products:
                return {
                    'products': ['暂无数据'],
                    'sales': [0]
                }
            
            # 反转列表，使图表从上到下显示排名从高到低
            products.reverse()
            sales.reverse()
            
            return {
                'products': products,
                'sales': sales
            }
            
        except Exception as e:
            print(f"获取热销商品排行数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'products': ['暂无数据'],
                'sales': [0]
            }
    
    def get_member_consumption(self):
        """获取会员等级消费分析数据
        
        返回各会员等级的消费情况
        """
        try:
            # 查询各会员等级的消费情况
            sql = """
            SELECT 
                ml.level_name,
                COUNT(DISTINCT u.id) as user_count,
                IFNULL(SUM(so.payment_amount), 0) as total_amount,
                IFNULL(SUM(so.payment_amount) / NULLIF(COUNT(DISTINCT so.user_id), 0), 0) as avg_amount
            FROM 
                member_level ml
            LEFT JOIN 
                py_user u ON (
                    SELECT SUM(payment_amount) 
                    FROM sales_order 
                    WHERE user_id = u.id
                ) BETWEEN ml.min_points AND ml.max_points
            LEFT JOIN 
                sales_order so ON u.id = so.user_id
            GROUP BY 
                ml.level_name, ml.id
            ORDER BY 
                ml.id
            """
            
            results = self.execute_query(sql)
            
            levels = []
            counts = []
            total_amounts = []
            avg_amounts = []
            
            for row in results:
                level = row[0]
                count = row[1] if row[1] is not None else 0
                total_amount = float(row[2]) if row[2] is not None else 0
                avg_amount = float(row[3]) if row[3] is not None else 0
                
                levels.append(level)
                counts.append(count)
                total_amounts.append(total_amount)
                avg_amounts.append(avg_amount)
            
            # 如果没有数据，返回默认数据
            if not levels:
                return {
                    'levels': ['暂无数据'],
                    'counts': [0],
                    'total_amounts': [0],
                    'avg_amounts': [0]
                }
            
            return {
                'levels': levels,
                'counts': counts,
                'total_amounts': total_amounts,
                'avg_amounts': avg_amounts
            }
            
        except Exception as e:
            print(f"获取会员等级消费分析数据出错: {e}")
            traceback.print_exc()
            # 返回默认数据而不是抛出异常
            return {
                'levels': ['暂无数据'],
                'counts': [0],
                'total_amounts': [0],
                'avg_amounts': [0]
            }
