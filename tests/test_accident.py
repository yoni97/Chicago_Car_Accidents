from datetime import datetime
from repositories.save_csv_to_mongo import init_car_accidents
from tests.conftest import mongo_client, read_csv, csv_file




def init_car_accidents(db, csv_path):
    accidents_area = db['accidents_area']
    injuries = db['injuries']
    accidents_area.drop()
    injuries.drop()

    row = next(read_csv(csv_path))

    injury = {
        'injuries_total': row['INJURIES_TOTAL'],
        'injuries_fatal': row['INJURIES_FATAL']
    }

    try:
        injury_id = injuries.insert_one(injury).inserted_id
    except Exception as e:
        print(f"Error inserting injury: {e}")

    accident_area = {
        'beet_of_occurrence': row['BEAT_OF_OCCURRENCE'],
        'crash_date': row['CRASH_DATE'],
        'contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
        'injury_id': injury_id
    }

    try:
        accidents_area.insert_one(accident_area).inserted_id
    except Exception as e:
        print(f"Error inserting accident_area: {e}")

def test_init_car_accidents(mongo_client, csv_file):
    init_car_accidents(mongo_client, csv_file)

    accidents_area = mongo_client['accidents_area']
    injuries = mongo_client['injuries']

    assert accidents_area.count_documents({}) == 1
    assert injuries.count_documents({}) == 1
    accident = accidents_area.find_one({'beet_of_occurrence': '225'})
    assert accident is not None
    assert accident['contributory_cause'] == "UNABLE TO DETERMINE"









def count_accident_by_beat(area, accidents_area):
    res = accidents_area.count_documents({'beet_of_occurrence': str(area)})
    return {
        'beat': area,
        'count accidents': res
    }


def test_count_accident_by_beat(mongo_client):
    accidents_area = mongo_client['accidents_area']

    accidents_area.insert_many([
        {'beet_of_occurrence': '1234', 'crash_date': '2022-10-01'},
        {'beet_of_occurrence': '1234', 'crash_date': '2022-10-02'},
        {'beet_of_occurrence': '5678', 'crash_date': '2022-10-03'}
    ])

    result = count_accident_by_beat('1234', accidents_area)
    assert result['count accidents'] == 2
    assert result['beat'] == '1234'

    result = count_accident_by_beat('5678', accidents_area)
    assert result['count accidents'] == 1
    assert result['beat'] == '5678'

    result = count_accident_by_beat('9999', accidents_area)
    assert result['count accidents'] == 0
    assert result['beat'] == '9999'





















def count_accident_by_period(area, start_date, end_date, accidents_area):
    return accidents_area.count_documents({
        'beet_of_occurrence': area,
        'crash_date': {
            '$gte': start_date,
            '$lt': end_date
        }
    })


def test_count_accident_by_period(mongo_client):
    accidents_area = mongo_client['accidents_area']

    accidents_area.insert_many([
        {'beet_of_occurrence': '1234', 'crash_date': '2023-01-15'},
        {'beet_of_occurrence': '1234', 'crash_date': '2023-01-20'},
        {'beet_of_occurrence': '1234', 'crash_date': '2023-02-05'},
        {'beet_of_occurrence': '5678', 'crash_date': '2023-01-25'}
    ])

    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-01-31', '%Y-%m-%d')

    result = count_accident_by_period('1234', start_date, end_date, accidents_area)
    assert result != 2

    start_date = datetime.strptime('2023-02-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-02-28', '%Y-%m-%d')

    result = count_accident_by_period('1234', start_date, end_date, accidents_area)
    assert result != 1

    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-01-31', '%Y-%m-%d')

    result = count_accident_by_period('5678', start_date, end_date, accidents_area)
    assert result == 0

    start_date = datetime.strptime('2023-03-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-03-31', '%Y-%m-%d')

    result = count_accident_by_period('1234', start_date, end_date, accidents_area)
    assert result == 0





















def find_accident_by_area(beat, accidents_area):
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

def test_find_accident_by_area(mongo_client):
    accidents_area = mongo_client['accidents_area']

    accidents_area.insert_many([
        {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 1, 15), 'contributory_cause': 'Speeding'},
        {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 1, 20), 'contributory_cause': 'Speeding'},
        {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 2, 5), 'contributory_cause': 'Distraction'},
        {'beet_of_occurrence': '5678', 'crash_date': datetime(2023, 1, 25), 'contributory_cause': 'Distraction'}
    ])

    result = find_accident_by_area('1234', accidents_area)

    expected_result = [
        {
            '_id': 'Speeding',
            'count': 2,
            'accidents': [
                {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 1, 15), 'contributory_cause': 'Speeding'},
                {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 1, 20), 'contributory_cause': 'Speeding'}
            ]
        },
        {
            '_id': 'Distraction',
            'count': 1,
            'accidents': [
                {'beet_of_occurrence': '1234', 'crash_date': datetime(2023, 2, 5), 'contributory_cause': 'Distraction'}
            ]
        }
    ]

    assert result == expected_result






















def get_injury_stats_repo(beat, injuries):
    pipeline = [
        {"$lookup": {
            "from": "accidents_area",
            "localField": "injury_id",
            "foreignField": "_id",
            "as": "accident_area"
        }},
        {"$match": {"accident_area.beet_of_occurrence": beat}},
        {"$group": {
            "_id": None,
            "total_injuries": {"$sum": "$injuries_total"},
            "total_fatalities": {"$sum": "$injuries_fatal"},
            "total_non_fatal_injuries": {
                "$sum": {"$subtract": ["$injuries_total", "$injuries_fatal"]}
            }
        }},
        {"$project": {
            "_id": 0,
            "total_injuries": 1,
            "total_fatalities": 1,
            "total_non_fatal_injuries": 1
        }}
    ]
    result = list(injuries.aggregate(pipeline))
    return result[0] if result else {"message": "No data found for the given beat"}

def test_get_injury_stats_repo(mongo_client):
    accidents_area = mongo_client['accidents_area']
    injuries_area = mongo_client['injuries']
    injuries_area.insert_many([
        {'_id': 1, 'injuries_total': 2, 'injuries_fatal': 0, 'injury_id': 1},
        {'_id': 2, 'injuries_total': 3, 'injuries_fatal': 1, 'injury_id': 2}
    ])
    accidents_area.insert_many([
        {'_id': 1, 'beet_of_occurrence': '1234', 'injury_id': 1},
        {'_id': 2, 'beet_of_occurrence': '1234', 'injury_id': 2},
        {'_id': 3, 'beet_of_occurrence': '5678', 'injury_id': 1}
    ])

    result = get_injury_stats_repo('1234', injuries_area)

    expected_result = {
        'total_injuries': 5,
        'total_fatalities': 1,
        'total_non_fatal_injuries': 4
    }

    assert result == expected_result
    empty_result = get_injury_stats_repo('9999', injuries_area)
    assert empty_result == {"message": "No data found for the given beat"}
