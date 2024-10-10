from bson import ObjectId
from flask import Blueprint, jsonify, request

from db.database import injuries
from repositories.accident_repo import count_accident_by_beat, find_accident_by_area
from repositories.save_csv_to_mongo import init_car_accidents
from services.accident_services import sum_accidents_by_area_and_period

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
    crashes_list = list(injuries.aggregate([
        {"$lookup": {'from': 'accidents_area', 'localField': 'injury_id', 'foreignField': '_id', 'as': 'beat'}},
        {'$match': {'beat.beet_of_occurrence': beat}},
        {'$project':{'_id':0, 'injury_id':0, 'accidents_area._id': 0}},
            {'$group':{'_id':None,
                'injuries_total':{'$sum':'$injuries.injuries_total'},
                   'injuries_fatal': {'$sum': '$injuries.injuries_fatal'},
                   'total_non_fatal_injuries': {'$sum': {'$subtract': ['$injuries.injuries_total', '$injuries.injuries_fatal']}},
                   'crashes': {'$push': {'crash': '$$ROOT'}}}},
        {'$project':{'_id':0}}
    ]))
    return crashes_list



# def get_crash_stats(beat):
#     crashes_list = list(injuries.aggregate([
#         {
#             "$lookup": {
#                 'from': 'accidents',
#                 'localField': 'injury_id',
#                 'foreignField': 'injury_id',
#                 'as': 'accident_info'
#             }
#         },
#         {
#             "$unwind": {
#                 "path": "$accident_info",
#                 "preserveNullAndEmptyArrays": True
#             }
#         },
#         {
#             "$match": {
#                 'accident_info.beet_of_occurrence': beat
#             }
#         },
#         {
#             "$group": {
#                 '_id': None,
#                 'injuries_total': {'$sum': '$injuries_total'},
#                 'injuries_fatal': {'$sum': '$injuries_fatal'},
#                 'total_non_fatal_injuries': {
#                     '$sum': {'$subtract': ['$injuries_total', '$injuries_fatal']}
#                 },
#                 'crashes': {'$push': {
#                     'crash_date': '$accident_info.crash_date',
#                     'contributory_cause': '$accident_info.contributory_cause',
#                     'injury_id': '$injury_id'
#             }
#         },
#         {
#             "$project": {
#                 '_id': 0
#             }
#         }
#     ]))
#
#     return crashes_list
