from datetime import datetime, timedelta
from repositories.accident_repo import count_accident_by_period, count_accidents_by_cause, find_accident_by_id


def sum_accidents_by_area_and_period(area, date_str, period):
    base_date = datetime.strptime(date_str, '%d/%m/%Y')
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
        return {"error": "Invalid period. write 'day', 'week', or 'month'."}
    total_accidents = count_accident_by_period(area, start_date, end_date)

    return {
        'area': area,
        'total_accidents': total_accidents,
        'period': period,
        'date': date_str
    }

def get_accidents_groupe_by_cause(area):
    causes = ['FAILING TO REDUCE SPEED TO AVOID CRASH', 'UNDER THE INFLUENCE OF ALCOHOL/DRUGS']
    results = {}
    for cause in causes:
        count = count_accidents_by_cause(area, cause)
        results[cause] = count

    return results

def get_accident_details(id):
    accident = find_accident_by_id(id)
    if accident:
        return {
            'id': str(accident['_id']),
        }
    return None
