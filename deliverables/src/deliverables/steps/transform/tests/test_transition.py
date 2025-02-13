import pytest
from ...transform import CustomTransformStep, Transition

def create_mock_company_table(output):
    return (f"CREATE TABLE {output} (company_id INT, company_name VARCHAR(512));"
            f"INSERT INTO {output} VALUES (1001, 'Google'), (2002, 'Microsoft');")

TEST_COLUMNS = [
    "SCRAPE_TS", "USER_ID", "URI", "new_POSITION_ID", "new_COMPANY_RAW", "new_COMPANYURI",
    "new_TITLE_RAW", "new_LOCATION_RAW", "new_DESCRIPTION", "new_STARTDATE", "new_ENDDATE",
    "new_SEQUENCENO", "new_ORDER_IN_PROFILE", "new_CITY", "new_STATE", "new_STATE_ABBR",
    "new_COUNTRY", "new_METRO_AREA", "new_REGION_NAME", "new_RCID", "prev_POSITION_ID",
    "prev_COMPANY_RAW", "prev_COMPANYURI", "prev_TITLE_RAW", "prev_LOCATION_RAW",
    "prev_DESCRIPTION", "prev_STARTDATE", "prev_ENDDATE", "prev_SEQUENCENO",
    "prev_ORDER_IN_PROFILE", "prev_CITY", "prev_STATE", "prev_STATE_ABBR",
    "prev_COUNTRY", "prev_METRO_AREA", "prev_REGION_NAME", "prev_RCID"
]

def test_transition():
    transition_input = CustomTransformStep(custom_sql=create_mock_company_table)
    mock_table = Transition(input_companies=transition_input, columns=TEST_COLUMNS)
    mock_table.execute(cache=[], debug=False, test=True)
    assert mock_table.is_equal(mock_table, cache=[], debug=False, test=True)
