from ...transform import CustomTransformStep
from ...delivery import CopySQL
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
def test_copy_sql(name, columns):
    mock_table = CustomTransformStep(custom_sql=custom_query)
    cs = CopySQL(mock_table,
                       prod_location=f'TEST_RUN_TABLES.unload_sf_test',
                       test_location=f'TEST_RUN_TABLES.unload_sf_test_{name}',
                       columns=columns)
    cs.execute(cache=[], debug=False, test=True)
    cs.promote(debug=False, test=True)


def test_copy_sql_no_columns():
    mock_table = CustomTransformStep(custom_sql=custom_query)
    cs = CopySQL(mock_table,
                  prod_location=f'TEST_RUN_TABLES.unload_sf_test_prod_no_col',
                  test_location=f'TEST_RUN_TABLES.unload_sf_test_test_no_col')
    cs.execute(cache=[], debug=False, test=True)
    cs.promote(debug=False, test=True)
