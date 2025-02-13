from ...transform import CustomTransformStep, IndividualPosition
import pytest


def create_position_input_table(output):
    return (f"create table {output} (rcid int, company varchar(512));"
            f"insert into {output} values (12843, 'Revelio Labs'), (8030, 'Jane Street')")

TEST_COLUMNS = [
    "USER_ID",
    "POSITION_ID",
    "COMPANY_RAW",
    "TITLE_RAW",
    "LOCATION_RAW",
    "DESCRIPTION",
    "STARTDATE",
    "ENDDATE",
    "STATE",
    "COUNTRY",
    "METRO_AREA",
    "REGION_NAME",
    "RCID"]
def test_individual_position():
    position_input = CustomTransformStep(custom_sql=create_position_input_table)
    mock_table = IndividualPosition(input_table=position_input, columns=TEST_COLUMNS)
    mock_table.execute(cache=[], debug=False, test=True)
    assert mock_table.is_equal(mock_table, cache=[], debug=False, test=True)