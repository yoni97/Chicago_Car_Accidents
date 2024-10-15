import pymongo
import pytest

from db.database import accidents_area, injuries
from repositories.save_csv_to_mongo import read_csv


@pytest.fixture
def mongo_client():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['test_db']
    yield db
    client.drop_database('test_db')
    client.close()

@pytest.fixture
def crash_db_collection(init_db):
    crash_collection = init_db['crash_test']
    injuries_collection = init_db['injuries_test']
    return crash_collection, injuries_collection

@pytest.fixture
def csv_file(tmpdir):
    csv_content = """INJURIES_TOTAL,INJURIES_FATAL,BEAT_OF_OCCURRENCE,CRASH_DATE,PRIM_CONTRIBUTORY_CAUSE
    3,0,225,2022-10-01,UNABLE TO DETERMINE
    """
    csv_file = tmpdir.join("Traffic_Crashes.csv")
    csv_file.write(csv_content)
    return csv_file