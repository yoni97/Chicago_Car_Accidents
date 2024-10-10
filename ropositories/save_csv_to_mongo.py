import csv
from db.database import injuries, accidents_area


def read_csv(csv_path):
   with open(csv_path, 'r') as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           yield row


def init_taxi_drivers():
   accidents_area.drop()
   injuries.drop()

   for row in read_csv('../csv_files/Traffic_Crashes_-_Crashes - 20k rows'):

       injury = {
           'injuries_total': int(row['INJURIES_TOTAL']),
           'injuries_fatal': int(row['INJURIES_FATAL'])
       }

       try:
           injury_id = injuries.insert_one(injury).inserted_id
       except Exception as e:
           print(f"Error inserting driver: {e}")

       area = {
            'beet_of_occurrence': row['BEAT_OF_OCCURRENCE'],
            'crash_date': row['CRASH_DATE'],
            'contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
            'injury_id': injury_id
       }

       try:
           accidents_area.insert_one(area).inserted_id
       except Exception as e:
           print(f"Error inserting driver: {e}")




init_taxi_drivers()

