from ...transform import CustomTransformStep
from ...delivery import CopySnowflake
from ...table import Table
from ....utils import load_df, execute
import pytest

ALL = '*'
NONE = None
LIST_COL = ['col1', 'col2']
DICT_COL = {'col1': 'column1', 'col2': 'column2'}


def custom_query(output):
    return f"""
        drop table if exists {output};
        create table {output} (col1 BIGINT, col2 VARCHAR(512));
        INSERT INTO {output} VALUES 
        (1, 'test1'),
        (2, 'test2');
        """

@pytest.mark.parametrize("name, columns", [("all", ALL), ("none", NONE), ("list", LIST_COL), ("dict", DICT_COL)])
def test_copy_snowflake(name, columns):
    mock_table = CustomTransformStep(custom_sql=custom_query)
    cs = CopySnowflake(mock_table,
                       prod_location=f'ASSIGNMENT_DS_CFDE.TEST_RUN_TABLES.unload_sf_test',
                       test_location=f'ASSIGNMENT_DS_CFDE.TEST_RUN_TABLES.unload_sf_test_{name}',
                       columns=columns)
    cs.execute(cache=[], debug=False, test=True)
    cs.promote(debug=False, test=True)


def test_copy_snowflake_no_columns():
    mock_table = CustomTransformStep(custom_sql=custom_query)
    cs = CopySnowflake(mock_table,
                  prod_location=f'ASSIGNMENT_DS_CFDE.TEST_RUN_TABLES.unload_sf_test_prod_no_col',
                  test_location=f'ASSIGNMENT_DS_CFDE.TEST_RUN_TABLES.unload_sf_test_test_no_col')
    cs.execute(cache=[], debug=False, test=True)
    cs.promote(debug=False, test=True)
