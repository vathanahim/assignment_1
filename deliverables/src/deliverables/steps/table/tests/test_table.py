from ..table import Table


def test_table_sql():
    companies = Table(f"INGESTION.LOCATION_MAPPER_V3",
                      "INGESTION.LOCATION_MAPPER_V3")
    companies_table = companies.execute(cache=[], debug=False, test=True)
    assert companies.is_equal(companies, cache=[], debug=False, test=True)
    assert (companies_table == "INGESTION.LOCATION_MAPPER_V3")


