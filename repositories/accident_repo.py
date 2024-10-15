from db.database import accidents_area, injuries


def count_accident_by_beat(area):
    res = accidents_area.count_documents({'beet_of_occurrence': str(area)})
    return {
        'beat': area,
        'count accidents': res
    }

def count_accident_by_period(area, start_date, end_date):
    return accidents_area.count_documents({
        'beet_of_occurrence': area,
        'crash_date': {
            '$gte': start_date,
            '$lt': end_date
        }
    })

def find_accident_by_area(beat):
    query = [
        {"$match": {
            "beet_of_occurrence": beat
        }},
        {'$group': {
            '_id': '$contributory_cause',
            'count': {'$sum': 1},
            'accidents': {'$push': {
                'beet_of_occurrence': '$beet_of_occurrence',
                'crash_date': '$crash_date',
                'contributory_cause': '$contributory_cause',}}}},]
    results = list(accidents_area.aggregate(query))
    return results

def get_injury_stats_repo(beat):
    pipeline = [
        {"$lookup": {
            "from": "accidents_area","localField": "injury_id","foreignField": "_id","as": "accident_area"
        }},
        {"$match": {"accident_area.beet_of_occurrence": beat}},
        {"$group": {
            "_id": None,
            "total_injuries": {"$sum": "$injuries_total"},
            "total_fatalities": {"$sum": "$injuries_fatal"},
            "total_non_fatal_injuries": {
                "$sum": {"$subtract": ["$injuries_total", "$injuries_fatal"]}}}},
        {"$project": {
            "_id": 0,
            "total_injuries": 1,
            "total_fatalities": 1,
            "total_non_fatal_injuries": 1}}]
    result = list(injuries.aggregate(pipeline))
    return result[0] if result else {"message": "No data found for the given beat"}

def get_crashes_from_db(beat):
    return list(accidents_area.aggregate([
        {'$match': {'beet_of_occurrence': beat}},
        {"$lookup": {'from': 'injuries','localField': 'injury_id','foreignField': '_id','as': 'injuries'}},
        {'$unwind': '$injuries'},
        {'$group': {'_id': None,
            'injuries_total': {
                '$sum': {'$cond': [
                        {'$gt': [{'$strLenCP': '$injuries.injuries_total'}, 0]},
                        {'$toInt': '$injuries.injuries_total'},0]}},
            'injuries_fatal': {
                '$sum': {'$cond': [
                        {'$gt': [{'$strLenCP': '$injuries.injuries_fatal'}, 0]},
                        {'$toInt': '$injuries.injuries_fatal'},0]}},
            'total_non_fatal_injuries': {
                '$sum': {'$subtract': [
                        {'$ifNull': [{'$cond': [
                                {'$gt': [{'$strLenCP': '$injuries.injuries_total'}, 0]},
                                {'$toInt': '$injuries.injuries_total'},0]}, 0]},
                        {'$ifNull': [{'$cond': [
                                {'$gt': [{'$strLenCP': '$injuries.injuries_fatal'}, 0]},
                                {'$toInt': '$injuries.injuries_fatal'},0]}, 0]}]}},
            'crashes': {'$push': {'crash': '$$ROOT'}}}},
        {'$project': {'_id': 0}}]))
