from bson import ObjectId
from flask import Blueprint, jsonify, request

from db.database import accidents_area
from repositories.accident_repo import count_accident_by_beat
from repositories.save_csv_to_mongo import init_car_accidents
from services.accident_services import sum_accidents_by_area_and_period, get_accidents_groupe_by_cause, \
    get_accident_details

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

@accidents_bp.route('/accidents/cause', methods=['GET'])
def accidents_by_cause():
    area = request.args.get('area')
    if not area:
        return jsonify({"error": "Area parameter is required"}), 400
    results = get_accidents_groupe_by_cause(area)
    return jsonify(results)


@accidents_bp.route('/accidents/grouped/<string:beat>', methods=['GET'])
def get_accidents_by_cause(beat):
    try:
        query = [
            {"$match":{
            "beet_of_occurrence": beat
        }},
            {'$group': {
                '_id': 'contributory_cause',
                'count': {'$sum': 1},
                'accidents': {'$push': {
                    '_id': '_id',
                    'beet_of_occurrence': beat,
                    'crash_date': '$crash_date',
                    'contributory_cause': '$contributory_cause',
                    'injury_id': '$injury_id',
                }}
            }},
        ]
        results = list(accidents_area.aggregate(query))
        final_result = serialize_object(results)
        return jsonify(final_result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def serialize_object(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


# def get_accidents_by_cause(beat):
#     try:
#         accident_data = find_accidents(beat)
#         if not accident_data:
#             return jsonify({"message": "No accidents found for this beat"}), 404
#
#         for accident in accident_data:
#             accident['_id'] = str(accident['_id'])
#
#         return jsonify(accident_data), 200

# def accident(id):
#     accident_details = get_accident_details(id)
#     if accident_details is None:
#         return jsonify({"error": "Accident not found"}), 404
#     return jsonify(accident_details)
