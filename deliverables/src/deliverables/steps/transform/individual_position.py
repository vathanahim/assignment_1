from ..step import Step
from ...constants import SCHEMA_TEST, SCHEMA_PROD
from ...utils import load_df, execute
from ..table import Table

ALL_COLS = {
    "SCRAPE_TS",
    "USER_ID",
    "URI",
    "POSITION_ID",
    "COMPANY_RAW",
    "COMPANYURI",
    "TITLE_RAW",
    "LOCATION_RAW",
    "DESCRIPTION",
    "STARTDATE",
    "ENDDATE",
    "SEQUENCENO",
    "ORDER_IN_PROFILE",
    "CITY",
    "STATE",
    "COUNTRY",
    "METRO_AREA",
    "REGION_NAME",
    "RCID"
}


class IndividualPosition(Step):
    """
        A class to run an Individual Position step

        ...

        Attributes
        ----------
        input_table : Step
            input table, specifying which companies we're interested in
        columns : list
            list of columns in your pipeline, can be any column from ALL_COLS.
    """

    def __init__(self, input_table: Step, columns: list[str]):
        super().__init__(key="inflow_file")
        self.input_table = input_table
        self.columns = [col.lower() for col in columns]
        self.source_table = Table("INGESTION.INDIVIDUAL_POSITION",
                                  "INGESTION.INDIVIDUAL_POSITION")

    def _process_columns(self):
        processed_cols  = []
        for c in self.columns:
            if c.upper() not in ALL_COLS:
                raise Exception(f"Invalid column {c}")
            if c.upper() in ('RCID', 'COMPANY'):
                processed_cols.append("A.RCID")
            else:
                processed_cols.append(c.upper())
        return ', '.join(processed_cols)

    def _execute(self, cache, debug, test):
        source_table = self.source_table.execute(cache, debug, test)

        schema = SCHEMA_TEST if test else SCHEMA_PROD
        individual_position_file = f"{schema}.{self.step_id}_individual_position"
        col_str = self._process_columns()
        input_table = self.input_table.execute(cache, debug, test)

        print(f"""
========== EXECUTING STEP: INDIVIDUAL PIPELINE
Step ID:                {self.step_id}
Input table:            {input_table}
Columns:                {self.columns}
        """)

        if not debug:
            query = f"""
            drop table if exists {individual_position_file};
            CREATE TABLE {individual_position_file} AS
            SELECT {col_str}
            FROM {source_table} A
            JOIN {input_table} B
            ON A.RCID = B.RCID
            """

            # there wasn't a warehouse for transition so we're using the one for posting dynam instead (useless)
            execute(query)

        return individual_position_file

    def get_parent_step(self):
        return {"input_table": self.input_table, "source_table": self.source_table}

    def update_parent_step(self, new_parent, key=None):
        if key == "source_table":
            self.source_table = new_parent
        else:
            self.input_table = new_parent

    def is_equal(self, other, cache, debug, test) -> bool:
        return self.step_id == other.step_id or (
                isinstance(other, IndividualPosition)
                and self.compare_parent_keys(other, ignore=['big_transition_table'])
                and set(self.columns) == set(other.columns)
                and self.input_table.is_equal(other.input_table, cache, debug, test)
        )
