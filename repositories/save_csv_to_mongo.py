import csv
from datetime import datetime

from db.database import injuries, accidents_area


def read_csv(csv_path):
   with open(csv_path, 'r') as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           yield row

def parse_date(date_str: str):
    has_seconds = len(date_str.split(' ')) > 2
    date_format = '%m/%d/%Y %H:%M:%S %p' if has_seconds else '%m/%d/%Y %H:%M'
    return datetime.strptime(date_str, date_format)

def init_car_accidents():
   accidents_area.drop()
   injuries.drop()

   accidents_area.create_index('beet_of_occurrence')
   injuries.create_index('crash_date')

   for row in read_csv('csv_files/Traffic_Crashes.csv'):

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
            'crash_date': parse_date(row['CRASH_DATE']),
            'contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
            'injury_id': injury_id
       }
       try:
           accidents_area.insert_one(accident_area).inserted_id
       except Exception as e:
           print(f"Error inserting accident_area: {e}")




def init_car_accidents_big_data():
   accidents_area.drop()
   injuries.drop()
   injuries_list = []
   accidents_list = []

   for row in read_csv('csv_files/Traffic_Crashes_-_Crashes.csv'):

       injury = {
           'injuries_total': row['INJURIES_TOTAL'],
           'injuries_fatal': row['INJURIES_FATAL']
       }
       injuries_list.append(injury)

   try:
       injury_id = injuries.insert_one(injury).inserted_id
   except Exception as e:
       print(f"Error inserting injury: {e}")

   for row in read_csv('csv_files/Traffic_Crashes_-_Crashes.csv'):

       accident_area = {
            'beet_of_occurrence': row['BEAT_OF_OCCURRENCE'],
            'crash_date': row['CRASH_DATE'],
            'contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
            'injury_id': injury_id
       }
       accidents_list.append(accident_area)
   try:
       accidents_area.insert_many(accidents_list).inserted_ids
   except Exception as e:
       print(f"Error inserting accident_area: {e}")

   accidents_area.create_index('beet_of_occurrence')
   injuries.create_index('crash_date')



