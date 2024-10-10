# from re
# from tests.conftest import crash_db_collection
#
# def test_insert_crash(crash_db_collection):
#     csv_path = 'C:\\Users\y0504\Desktop\Week 5(10-10)\data\Traffic_Crashes_-_Crashes - 20k rows.csv'
#
#     csv_reader = read_csv(csv_path)
#     row = next(csv_reader)
#     injuries_document = injuries_info(row)
#     injuries_id = crash_db_collection[1].insert_one(injuries_document).inserted_id
#     document = crash_document(row, injuries_id)
#     crash_db_collection[0].insert_one(document)
#
#     assert injuries_id is not None
#     assert document is not None
#     assert crash_db_collection[0].count_documents({}) == 1
#     assert crash_db_collection[1].count_documents({'_id': injuries_id}) == 1

from services.accident_services import sum_accidents_by_area_and_period

def test_sum_accidents_by_area_and_period_valid_day(mocker):
    mock_count = mocker.patch('repositories.accident_repo.count_accidents_by_area_and_period', return_value=5)
    result = sum_accidents_by_area_and_period('225', '2024-10-01', 'day')

    assert result['total_accidents'] == 5
    assert result['area'] == '225'
    assert result['period'] == 'day'
    assert result['date'] == '2024-10-01'
    mock_count.assert_called_once_with('225', '2024-10-01', 'day')