from ....utils import execute, load_df
from ...transform import CustomTransformStep


def custom_query(output):
    return f"""
        drop table if exists {output};
        create table {output} (col1 BIGINT, col2 VARCHAR(512));
        INSERT INTO {output} VALUES 
        (1, 'test1'),
        (2, 'test2');
        """

def test_custom_transform():
    mock_table = CustomTransformStep(custom_sql=custom_query)
    output_table = mock_table.execute(cache=[], debug=False, test=True)
    data = load_df(f"select * from {output_table};")
    assert (data.shape == (2, 2))
    assert (list(data.columns) == ['col1', 'col2'])
    assert mock_table.is_equal(mock_table, cache=[], debug=False, test=True)
