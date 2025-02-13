import snowflake.connector

def snowflake_client(warehouse=None):
    connection = snowflake.connector.connect(
        user="test",
        password="test",
        account="test",
        database="assignment-ds-cfde",
        host="snowflake.localhost.localstack.cloud",  # For instances that run on the same machine as the client
        port=4566  # Use port 4566 for non-SSL, 443 for SSL connections
    )
    return connection.cursor()


def execute(sql_str):
    statements = sql_str.split(';')
    for s in statements:
        snowflake_client().execute(s)


def load_df(sql):
    cur = snowflake_client()
    cur.execute(sql)
    return cur.fetch_pandas_all()
