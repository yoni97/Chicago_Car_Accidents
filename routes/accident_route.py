from flask import Blueprint, jsonify, request
from repositories.accident_repo import count_accident_by_beat, find_accident_by_area
from repositories.save_csv_to_mongo import init_car_accidents, init_car_accidents_big_data
from services.accident_services import sum_accidents_by_area_and_period, get_crash_stats_service

accidents_bp = Blueprint('accidents_bp', __name__)

@accidents_bp.route('/')
def init_db():
    init_car_accidents()
    return jsonify('welcome to car accidends analystics!\n data is initial succesfully'), 201

@accidents_bp.route('/accidents/<string:area>', methods=['GET'])
def get_accident_by_beat(area):
    beat = count_accident_by_beat(area)
    return jsonify(beat)

@accidents_bp.route('/accidents/period', methods=['GET'])
def sum_accidents():
    data = request.get_json()
    area = data.get('area')
    date_str = data.get('date')
    period = data.get('period')  # "day", "week", "month"

    if not area or not date_str or not period:
        return jsonify({"error": "Missing area, date or period parameter"}), 400
    result = sum_accidents_by_area_and_period(area, date_str, period)
    return jsonify(result)

@accidents_bp.route('/accidents/cause/<string:beat>', methods=['GET'])
def get_accidents_by_cause(beat):
    try:
        result = find_accident_by_area(beat)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@accidents_bp.route('/injuries/cause/<string:beat>', methods=['GET'])
def get_crash_stats(beat):
    crashes_list = get_crash_stats_service(beat)
    return jsonify(crashes_list)






