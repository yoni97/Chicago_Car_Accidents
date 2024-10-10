import pymongo
import pytest


@pytest.fixture
def init_db():
    client = pymongo.MongoClient('localhost', 27017)
    test_db = client['test_db']
    yield test_db
    client.drop_database('test_db')
    client.close()

@pytest.fixture
def crash_db_collection(init_db):
    crash_collection = init_db['crash_test']
    injuries_collection = init_db['injuries_test']
    return crash_collection, injuries_collection