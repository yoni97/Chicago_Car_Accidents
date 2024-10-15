from datetime import datetime, timedelta
from repositories.accident_repo import count_accident_by_period, get_injury_stats_repo, get_crashes_from_db
from bson import ObjectId



def sum_accidents_by_area_and_period(area, date_str, period):
    try:
        base_date = datetime.strptime(date_str, '%d/%m/%Y')
    except ValueError:
        try:
            base_date = datetime.strptime(date_str, '%m/%d/%Y')
        except ValueError:
            return {"error": "Invalid date format. Please use DD/MM/YYYY or MM/DD/YYYY"}

    base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == 'day':
        start_date = base_date
        end_date = base_date + timedelta(days=1)
    elif period == 'week':
        start_date = base_date
        end_date = base_date + timedelta(weeks=1)
    elif period == 'month':
        start_date = base_date
        end_date = base_date + timedelta(days=30)
    else:
        return {"error": "Invalid period. write 'day', 'week', or 'month'"}

    print(start_date, end_date)
    total_accidents = count_accident_by_period(area, start_date, end_date)

    return {
        'area': area,
        'total_accidents': total_accidents,
        'period': period,
        'date': date_str
    }


def get_injury_stats_service(beat):
    return get_injury_stats_repo(beat)

def convert_object_ids(obj):
    if isinstance(obj, list):
        return [convert_object_ids(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_object_ids(value) for key, value in obj.items()}
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def get_crash_stats_service(beat):
    crashes_list = get_crashes_from_db(beat)
    for crash in crashes_list:
        crash['crashes'] = [
            {**c, 'injury_id': str(c['injury_id'])} if 'injury_id' in c else c for c in crash['crashes']
        ]
        for key in crash.keys():
            if isinstance(crash[key], ObjectId):
                crash[key] = str(crash[key])
    return convert_object_ids(crashes_list)


