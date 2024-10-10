from db.database import accidents_area


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
                'contributory_cause': '$contributory_cause',
            }}
        }},
    ]
    results = list(accidents_area.aggregate(query))
    return results