from ..step import Step
from ...constants import SCHEMA_TEST, SCHEMA_PROD
from ...utils import load_df, execute
from ..table import Table

class Transition(Step):
    """
    A class to run a Transition step

    ...

    Attributes
    ----------
    input_companies : Step
        input table, specifying which companies we're interested in
    columns : list
        list of basic column names that will be prefixed with prev_ and new_
    """

    def __init__(self, input_companies: Step, columns: list[str]):
        super().__init__(key="inflow_file")
        self.input_companies = input_companies
        self.columns = [col.lower() for col in columns]
        self.source_table = Table("INGESTION.TRANSITION_UNIQUE_BASE",
                                "INGESTION.TRANSITION_UNIQUE_BASE")

    def _process_columns(self):
        base_cols = ['prev_company_id', 'new_company_id', 'prev_position_id', 'new_position_id']
        
        # Add prefixed versions of user columns
        processed_cols = []
        for c in self.columns:
            prev_col = f'prev_{c}'.upper()
            new_col = f'new_{c}'.upper()
            
            # Check if columns exist in source table
            check_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'TRANSITION_UNIQUE_BASE'
                AND column_name IN ('{prev_col}', '{new_col}')
            """
            existing_cols = load_df(check_query)['column_name'].tolist()
            if prev_col in existing_cols and new_col in existing_cols:
                processed_cols.extend([prev_col, new_col])
                
        return ', '.join(base_cols + processed_cols)

    def _execute(self, cache, debug, test):
        source_table = self.source_table.execute(cache, debug, test)
        
        schema = SCHEMA_TEST if test else SCHEMA_PROD
        inflow_file = f"{schema}.{self.step_id}_inflow"
        outflow_file = f"{schema}.{self.step_id}_outflow"
        col_str = self._process_columns()
        input_companies = self.input_companies.execute(cache, debug, test)

        print(f"""
========== EXECUTING STEP: TRANSITION PIPELINE
Step ID:                {self.step_id}
Input companies:        {input_companies}
Columns:               {self.columns}
        """)

        if not debug:
            # Create inflow file
            inflow_query = f"""
            DROP TABLE IF EXISTS {inflow_file};
            CREATE TABLE {inflow_file} AS
            SELECT {col_str}
            FROM {source_table} t
            INNER JOIN {input_companies} c
                ON t.new_company_id = c.company_id
            WHERE t.prev_company_id != t.new_company_id
            """
            
            # Create outflow file
            outflow_query = f"""
            DROP TABLE IF EXISTS {outflow_file};
            CREATE TABLE {outflow_file} AS
            SELECT {col_str}
            FROM {source_table} t
            INNER JOIN {input_companies} c
                ON t.prev_company_id = c.company_id
            WHERE t.prev_company_id != t.new_company_id
            """

            execute(inflow_query)
            execute(outflow_query)

        return {
            'inflow_file': inflow_file,
            'outflow_file': outflow_file
        }

    def get_parent_step(self):
        return {"input_companies": self.input_companies, "source_table": self.source_table}

    def update_parent_step(self, new_parent, key=None):
        if key == "source_table":
            self.source_table = new_parent
        else:
            self.input_companies = new_parent

    def is_equal(self, other, cache, debug, test) -> bool:
        return self.step_id == other.step_id or (
                isinstance(other, Transition)
                and self.compare_parent_keys(other, ignore=['big_transition_table'])
                and set(self.columns) == set(other.columns)
                and self.input_companies.is_equal(other.input_companies, cache, debug, test)
        )