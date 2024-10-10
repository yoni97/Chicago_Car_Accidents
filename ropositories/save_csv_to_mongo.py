import csv
from db.database import injuries, accidents_area, db


def read_csv(csv_path):
   with open(csv_path, 'r') as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           yield row


def init_car_accidents():
   accidents_area.drop()
   injuries.drop()

   try:
       db.accidents_area.create_index('beet_of_occurrence', 1)
       db.injuries.create_index('crash_date', 1)
       print("Indexes created successfully")
   except Exception as e:
       print(f"Error creating indexes: {e}")

   for row in read_csv('../csv_files/Traffic_Crashes.csv'):

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




init_car_accidents()

