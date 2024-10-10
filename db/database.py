from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017")
car_accidents = client['car-accidents']

injuries = car_accidents['INJURIES']
cars = car_accidents['OCCURRENCES']