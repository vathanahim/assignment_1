from ..step import Step
from ...utils import execute, load_df
from datetime import datetime, timedelta
from ...constants import DAILY_BATCHTIME


class Table(Step):
    def __init__(self, prod_name, test_name, redshift:bool=False, warehouse:str = "15TH_AUTOMATION_XL",
                 prod_name_property:bool = False):
        super().__init__(is_comp_via_exec=True, warehouse=warehouse)
        if not prod_name_property:
            self.prod_name = prod_name
        self.test_name = test_name
        self.redshift = redshift

    def __repr__(self):
        return f'\n\tTable(prod_name: {self.prod_name}, test_name: {self.test_name})\n\t\t'

    def _parse_name_vars(self, name):
        daily_batchtime = getattr(self, 'daily_batchtime', DAILY_BATCHTIME)
        daily_batchtime = DAILY_BATCHTIME if daily_batchtime is None else daily_batchtime
        parsed_name = name.replace('$prev_daily_batchtime$', (datetime.strptime(daily_batchtime, '%Y%m%d') - timedelta(days=1)).strftime("%Y%m%d"))
        return parsed_name

    @property
    def prod_name_parsed(self):
        return self._parse_name_vars(self.prod_name)

    @property
    def test_name_parsed(self):
        return self._parse_name_vars(self.test_name)

    def get_name(self, test=None):
        test_ret = test if test else self.test
        name = self.test_name if test_ret else self.prod_name
        return name

    def get_name_parsed(self, test=None):
        test_ret = test if test else self.test
        name = self.test_name_parsed if test_ret else self.prod_name_parsed
        return name

    def _execute(self, cache, debug, test):
        prod_tbl = load_df(f"SELECT * FROM {self.prod_name_parsed} WHERE 0=1;")
        test_tbl = load_df(f"SELECT * FROM {self.test_name_parsed} WHERE 0=1;")
        if test and sorted(list(prod_tbl.columns)) != sorted(list(test_tbl.columns)):
            prod_only = [c for c in prod_tbl.columns if c not in test_tbl.columns]
            test_only = [c for c in test_tbl.columns if c not in prod_tbl.columns]
            raise AssertionError(f"Columns are not equal: {prod_only} only in {self.prod_name_parsed}, {test_only} only in {self.test_name_parsed}")

        return self.get_name_parsed(test)

    def get_parent_step(self):
        return None

    def update_parent_step(self, new_parent, key=None):
        pass

    def is_equal(self, other, cache, debug, test) -> bool:
        return (self.step_id == other.step_id) or (
            isinstance(other, Table)
            and self.compare_parent_keys(other)
            and self.get_name(test) == other.get_name(test)
            and self.redshift == other.redshift
        ) or (not self.redshift and self.compare_via_execution(other, cache, debug, test) == "EQUAL")


class VersionedTable(Table):
    def __init__(self, prod_table, test_name, redshift:bool=False, warehouse:str = "15TH_AUTOMATION_XL",
                 prod_database:str=None, enforce_version=None):
        super().__init__(prod_table, test_name, redshift, warehouse, prod_name_property=True)
        self.prod_table = prod_table
        self.enforce_version = enforce_version
        self.data_version = "CURRENT"

    @property
    def prod_name(self):
        if not self.data_version: self.data_version = "CURRENT"
        version = self.data_version if not self.enforce_version else self.enforce_version
        return f"OUTPUT_{version.upper()}.{self.prod_table}"

    def _execute(self, cache, debug, test):
        print(f"INSIDE VersionedTable, name is {self.get_name(test)}")
        return super()._execute(cache, debug, test)
