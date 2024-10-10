from datetime import datetime, timedelta
from repositories.accident_repo import count_accident_by_period


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




