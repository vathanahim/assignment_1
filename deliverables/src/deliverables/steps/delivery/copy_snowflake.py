from ..step import Step
from ...utils import execute, batchtime_diff, load_df
from ...constants import DATABASE, SCHEMA_TEST_PROMOTE, SCHEMA_PROD_PROMOTE, BATCHTIME, PREV_BATCHTIME, DAILY_BATCHTIME


class CopySnowflake(Step):
    """
       A class to copy snowflake table to the desired client's snowflake database

       ...

       Attributes
       ----------
       table : Step
           table to copy
       prod_location : str
           snowflake_database.schema.table location in prod mode
       test_location : str
           snowflake_database.schema.table location in test mode
       columns : Union[dict, list, str]
            columns to copy over in the target location, can be '*', None, list of str, dict of
            col: rename_col.
       skip_schema_check: bool = False
            set to True to disable schema check
        reference_table: str = None
            specific table to compare with for schema check
       prod_rename_existing: str = None
            name of existing table in prod we are trying to save the current table to if provided
       test_rename_existing: str = None
            name of existing table in test we are trying to save the current table to if provided
       truncate_table: bool = False
          whether to run truncate existing table and insert instead of create or replace.
       append_to_table: bool = False
          if this is set to true, the data will be appended to the destination table instead of
          replacing it when promoting
       """

    def __init__(self, table, prod_location, test_location, columns='*',
                 skip_schema_check: bool = False, reference_table:str = None, compare_parsed_is_equal = True,
                 prod_rename_existing = None, test_rename_existing = None, truncate_table:bool = False,
                 append_to_table=False):
        super().__init__()
        if isinstance(columns, dict) and (truncate_table or append_to_table):
            raise ValueError(f'Renaming columns are not supported when truncating or appending to an existing destination table.')
        self.table = table
        self.prod_location = prod_location
        self.test_location = test_location
        self.columns = columns
        self.skip_schema_check = skip_schema_check
        self.reference_table = reference_table
        self.prod_rename_existing = prod_rename_existing
        self.test_rename_existing = test_rename_existing
        if truncate_table and append_to_table:
            raise Exception("append_to_table and truncate_table are not compatible")
        self.truncate_table = truncate_table
        self.append_to_table = append_to_table
        self.compare_parsed_is_equal = compare_parsed_is_equal

    def __str__(self):
        return f"Copy to Snowflake table {self.prod_location}"

    def _parse_location_vars(self, location):
        daily_batchtime = getattr(self, 'daily_batchtime', DAILY_BATCHTIME)
        daily_batchtime = DAILY_BATCHTIME if daily_batchtime is None else daily_batchtime
        parsed_location = location.replace('$daily_batchtime$', daily_batchtime)
        return parsed_location

    @property
    def prod_location_parsed(self):
        return self._parse_location_vars(self.prod_location)

    @property
    def test_location_parsed(self):
        return self._parse_location_vars(self.test_location)

    def _process_columns(self):
        """
        Return string format of SELECT columns, a list of final column names, boolean whether schema is passed to the step
        """
        if self.columns is None or self.columns == '*':
            return '*', [], False
        if isinstance(self.columns, list) and isinstance(self.columns[0], tuple):
            cols = []
            for c in self.columns:
                if len(c) == 2:
                    cols.append((c[0], c[1], c[0]))
                else:
                    cols.append(c)
            return ',\n                        '.join(f'{v}::{t} as {rename.upper()}' for v, t, rename in cols), [rename.upper() for _,_,rename in cols],True
        if isinstance(self.columns, list):
            return ',\n                        '.join(f'{v} as {v.upper()}' for v in self.columns), [v.upper() for v in self.columns], False
        if isinstance(self.columns, dict):
            return ',\n                        '.join(f'{k} as {v.upper()}' for k, v in self.columns.items()), [v.upper() for _,v in self.columns.items()],False

    def _quality_check(self, test):
        # check the table exists
        try:
            dest_df = load_df(f"SELECT * FROM {self.location} LIMIT 1")
        except:
            raise RuntimeError(f"The table {self.location} wasn't properly created by this Copy to Snowflake step")
        if self.columns == '*':
            source_df = load_df(f"SELECT * FROM {self.temporary_table} LIMIT 1")
            assert (sorted(list(dest_df.columns)) == sorted(list(source_df.columns)))
        dest_count = list(load_df(f"SELECT count(*) AS cnt FROM {self.location}")['CNT'])[0]
        source_count = list(load_df(f"SELECT count(*) AS cnt FROM {self.temporary_table}")['CNT'])[0]

        if self.append_to_table:
            assert(dest_count == source_count + self.dst_count_prior)
        else:
            assert (dest_count == source_count)

    @property
    def temporary_table(self):
        schema = f'{DATABASE}.{SCHEMA_TEST_PROMOTE}' if self.test else f'{DATABASE}.{SCHEMA_PROD_PROMOTE}'
        location = self.test_location_parsed if self.test else self.prod_location_parsed
        id_str = location.replace('.', '_')
        output = f"{schema}.{id_str}_copy_snowflake"
        return output

    def _get_columns(self, table):
        get_columns_query = f"""
            select * from {table} where 1=0
        """
        columns_df = load_df(get_columns_query)
        return list(columns_df.columns)

    def _is_table_object(self, table):
        db, schema, name = table.split(".")
        table_type_query = f"""
            select table_type
            from {db}.information_schema.tables
            where table_catalog = '{db.upper()}' and table_schema = '{schema.upper()}' and table_name = '{name.upper()}';
        """
        table_type_df = load_df(table_type_query)
        if table_type_df.shape[0] != 1:
            raise Exception(f"{table} does not exist in the info schema.")

        return table_type_df['table_type'][0] == 'BASE TABLE'

    def _execute(self, cache, debug, test):
        data_table = self.table.execute(cache, debug, test)
        columns, schema_passed, _ = self._process_columns()
        print(f"""
========== EXECUTING STEP: COPY TO SNOWFLAKE
Step ID                 {self.step_id}
Delivering table        {data_table}
to temporary location   {self.temporary_table}
with columns            {columns}
""")
        if not debug:
            query = f"""
                    drop table if exists {self.temporary_table};
                    create or replace table {self.temporary_table} clone
                    {data_table};
                    """
            print(query)
            execute(query)
            if not schema_passed:
                print(f"Schema is checked!")
                prod = self.prod_location if not self.reference_table else self.reference_table
            else:
                print(f"Schema is NOT checked as schema is passed!")
        return self.temporary_table

    def _promote(self, debug, test):
        self.location = self.test_location_parsed if self.test else self.prod_location_parsed
        self.rename_existing = self.test_rename_existing if self.test else self.prod_rename_existing
        print(f"""
        ========== EXECUTING STEP: PROMOTE TO SNOWFLAKE
        Step ID                 {self.step_id}
        Delivering table        {self.temporary_table}
        to location             {self.location}
        and rename 
        existing table to       {self.rename_existing}
        truncate table?         {self.truncate_table}
        append to table?        {self.append_to_table}
        """)
        if not debug:
            query = ""
            cleanup_tables = [self.location]
            location_breakdown = self.location.split('.')
            location_db, location_schema, location_table = location_breakdown[0], location_breakdown[1], \
                location_breakdown[2]
            table_exists = True
            if self.rename_existing:
                if table_exists:
                    query += f"drop table if exists {self.rename_existing};"
                query += f"alter table if exists {self.location} rename to {self.rename_existing};"
                cleanup_tables.append(self.rename_existing)

            if (self.truncate_table or self.append_to_table) and table_exists:
                self.dst_count_prior = list(load_df(f"SELECT count(*) AS cnt FROM {self.location}")['CNT'])[0]
                select_columns, insert_columns, _ = self._process_columns()
                if select_columns == "*": # get final columns from temp table
                    columns = self._get_columns(self.location)
                    insert_columns, select_columns = columns, ','.join(columns)
                copy_query = ''
                if self.truncate_table:
                    copy_query = copy_query + f'truncate table {self.location};'
                copy_query = copy_query + f"""
                    insert into {self.location} ({','.join(insert_columns)}) 
                    select {select_columns} from {self.temporary_table};
                """
            else:
                copy_query = f"""
                    drop table if exists {self.location};
                    create or replace transient table {self.location} clone {self.temporary_table};
                """
            query += copy_query
            print(query)
            execute(query)
            self._quality_check(test)
            print(f"Dropping promote candidate {self.temporary_table}")
            execute(f"drop table {self.temporary_table};")

            ret = getattr(self, "snowflake_retention", None)
            if ret:
                print("Deleting older deliveries")
                for t in cleanup_tables:
                    for c in [PREV_BATCHTIME, BATCHTIME]:
                        if c in t:
                            t_older = t.replace(c, batchtime_diff(c, -ret))
                            if t_older not in getattr(self, "snowflake_cleanup_exceptions", []):
                                print(f"Dropping older table {t_older}")
                                execute(f"drop table if exists {t_older};")

        return self.location

    def get_parent_step(self):
        return self.table

    def update_parent_step(self, new_parent, key=None):
        self.table = new_parent

    def is_equal(self, other, cache, debug, test) -> bool:
        location_attr = ("test" if test else "prod") + "_location" + (
            "_parsed" if getattr(self, "compare_parsed_is_equal", True) else "")
        return (self.step_id == other.step_id) or (
                isinstance(other, CopySnowflake)
                and self.compare_parent_keys(other)
                and getattr(self, location_attr, "") == getattr(other, location_attr, "")
                and (self.test_location == other.test_location
                     if test
                     else self.prod_location == other.prod_location)
                and self.table.is_equal(other.table, cache, debug, test)
                and self.skip_schema_check == other.skip_schema_check
                and self.reference_table == other.reference_table
                and self.truncate_table == other.truncate_table
                and getattr(self, 'append_to_table', False) == getattr(other, 'append_to_table', False)
        )
