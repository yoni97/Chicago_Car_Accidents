from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/?directConnection=true")
db = client['car-accidents']

accidents_area = db['accidents_area']
injuries = db['injuries']


