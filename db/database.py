from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017")
car_accidents = client['car-accidents']

accidents_area = car_accidents['accidents_area']
injuries = car_accidents['injuries']