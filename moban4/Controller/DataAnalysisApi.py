from flask import Blueprint, jsonify
from Dao.DataAnalysisConnect import DataAnalysisDao
from Predictive.SalesPrediction import SalesPredictionModel
import traceback
from datetime import datetime

# 创建蓝图
data_analysis_api = Blueprint('data_analysis_api', __name__)
dao = DataAnalysisDao()

@data_analysis_api.route('/sales_trend', methods=['GET'])
def get_sales_trend():
    """获取销售趋势数据"""
    try:
        result = dao.get_sales_trend()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/user_distribution', methods=['GET'])
def get_user_distribution():
    """获取用户地域分布数据"""
    try:
        result = dao.get_user_distribution()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/category_sales', methods=['GET'])
def get_category_sales():
    """获取商品类别销售占比数据"""
    try:
        result = dao.get_category_sales()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/channel_analysis', methods=['GET'])
def get_channel_analysis():
    """获取销售渠道分析数据"""
    try:
        result = dao.get_channel_analysis()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/user_behavior', methods=['GET'])
def get_user_behavior():
    """获取用户行为分析数据"""
    try:
        result = dao.get_user_behavior()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/payment_methods', methods=['GET'])
def get_payment_methods():
    """获取支付方式分布数据"""
    try:
        result = dao.get_payment_methods()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/hot_products', methods=['GET'])
def get_hot_products():
    """获取热销商品排行数据"""
    try:
        result = dao.get_hot_products()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/member_consumption', methods=['GET'])
def get_member_consumption():
    """获取会员等级消费分析数据"""
    try:
        result = dao.get_member_consumption()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_analysis_api.route('/sales_prediction', methods=['GET'])
def get_sales_prediction():
    """获取销售预测数据"""
    try:
        model = SalesPredictionModel()
        predictions, error = model.predict_future_sales(days=30)
        
        if predictions:
            return jsonify(predictions)
        else:
            return jsonify({
                'error': error,
                'dates': [datetime.now().strftime('%m-%d')],
                'historical': [0],
                'predicted': [0],
                'prediction_start_index': 0,
                'model_type': 'N/A',
                'accuracy': 0,
                'mse': 0,
                'r2_score': 0,
                'growth_rate': 0,
                'predicted_total': 0,
                'conclusion': '无法生成预测，请确保有足够的历史数据。'
            }), 500
    except Exception as e:
        print(f"销售预测API错误: {e}")
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'dates': [datetime.now().strftime('%m-%d')],
            'historical': [0],
            'predicted': [0],
            'prediction_start_index': 0,
            'model_type': 'N/A',
            'accuracy': 0,
            'mse': 0,
            'r2_score': 0,
            'growth_rate': 0,
            'predicted_total': 0,
            'conclusion': '预测过程中发生错误，请稍后再试。'
        }), 500
