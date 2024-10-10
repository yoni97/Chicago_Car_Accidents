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

def count_accidents_by_cause(area, cause):
    return accidents_area.count_documents({
        'beet_of_occurrence': area,
        'contributory_cause': cause
    })

def find_accident_by_id(id):
    return accidents_area.find_one({'_id': id})