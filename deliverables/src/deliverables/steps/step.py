import copy
import traceback
import datetime

from ..constants import BATCHTIME, RUN_MODE_DASHBOARD
from ..utils import random_string, execute, load_df

DUPLICATE_CHECK_THRESHOLD = 0 #1000000

class WaitingException(Exception):
    def __init__(self, step):
        self.step = step


class FailedException(Exception):
    def __init__(self, step):
        self.step = step


class EmptyOutputException(Exception):
    def __init__(self, step):
        self.step = step

class DuplicateException(Exception):
    def __init__(self, step):
        self.step = step

class StepError(Exception):
    pass


class Step:
    def __init__(self, key="__out__", is_comp_via_exec=False, warehouse="15TH_AUTOMATION_XL", data_version=None):
        self.output_tables = {}
        self.key = key
        self.test = False
        self.is_comp_via_exec = is_comp_via_exec
        self.step_id = random_string()
        self.qa = None
        self.time_completed = None
        self.remove_self = False
        self.batchtime = BATCHTIME
        self.warehouse = warehouse
        self.data_version = data_version

    @property
    def step_print(self):
        return f"Step ID {self.step_id} ({self.__class__.__name__})"

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'

    def execute(self, cache, debug, test):
        step = -self if test else +self
        return step.execute_(cache, debug, test)

    def promote(self, debug, test):
        step = -self if test else +self
        return step.promote_(debug, test)

    def _check_output(self, output):
        """
        rowcount and duplicates are checked for every step except pipeline + delivery steps
        """
        import inspect
        import deliverables.steps.delivery as d
        # exclude delivery steps
        cls = []

        for name, data in inspect.getmembers(d):
            if name.startswith("__"):
                continue
            if 'class' in str(data):
                cls.append(name)
        cls.extend(['PrecomputeTimescaling'])

        # skip for delivery steps
        for cl in cls:
            if self.__class__.__name__ == cl:
                return -1, False

        tables = {}
        if not output:
            return -1, False
        if isinstance(output, str):
            tables['output_table'] = output
        if isinstance(output, dict):
            for k, v in output.items():
                if k in ['output_table', 'input_table', 'ref_table', 'pos_input_table', 'v1_input_table', 'inflow_file', 'outflow_file', 'posting_file',
                         "position_input",
                         "position_unique_base",
                         "position_unique_final",
                         "granularity",
                         "granularity_map",
                         "count_long",
                         "long_file",
                         "diagnostics_scaling",
                         "diagnostics_counts",
                         "granularity_skill",
                         "position_unique_skill",
                         "skill_count_long",
                         "skill_long_file",
                         ]:
                    tables[k] = v
        if not tables:
            return -1, False

        msg = ''
        res = 0
        duplicate = False

        for k, table_name in tables.items():
            query = f"SELECT COUNT(*) as c FROM {table_name};"
            result = load_df(query)
            c = result.iloc[0]['c']

            # Count distinct rows in a subquery when row counts in under a threshold
            if c <= DUPLICATE_CHECK_THRESHOLD:
                query_distinct = f"SELECT COUNT(*) as dc FROM (SELECT DISTINCT * FROM {table_name}) as a;"
                result_distinct = load_df(query_distinct)
                dc = result_distinct.iloc[0]['dc']
                msg += f'## {self.step_print}: {k}: rowcount is {c},  distinct rowcount is {dc}\n'
                duplicate = duplicate or (c > dc)
            else:
                msg += f'## {self.step_print}: {k}: rowcount is {c}\n'

            if c == 0:
                return c, False
            res = max(res, c)

        print(msg)
        return res, duplicate


    def execute_(self, cache, debug, test):
        """
        cache is a list that contains completed steps
        """
        self.is_comp_via_exec = False
        self.test = test
        for step, status, is_newly_added in cache:
            cur_status = status
            if self.is_equal(step, cache, debug, test):
                if cur_status == "RUNNING":
                    print(f"The equal step {step.step_id} is running. Step id {self.step_id} will be retried later")
                    raise WaitingException(step)
                if cur_status == "FAILED":
                    print(f"Step {self.step_id} failed because equivalent step {step.step_id} failed too")
                    raise FailedException(step)
                elif cur_status == "COMPLETED":
                    self.output_tables = step.output_tables
                    break
                else:
                    raise RuntimeError(f"cache status {status} not recognized")
        if not self.output_tables:
            cache.append((self, "RUNNING", True))
            try:
                output = self._execute(cache, debug, test)
                if not debug:
                    count, duplicate = self._check_output(output)
                    if count == 0:
                        print(f'Step {self.step_id} failed because the output table was empty.')
                        raise EmptyOutputException(self)
                    if duplicate: 
                        print(f'Step {self.step_id} failed because the output table has duplicated rows.')
                        raise DuplicateException(self)
                print(f"""
Step ID {self.step_id} ({self.__class__.__name__}): Execution complete
""")

            except WaitingException as e:
                cache.remove((self, "RUNNING", True))
                raise e
            except FailedException as e:
                cache.append((self, "FAILED", True))
                cache.remove((self, "RUNNING", True))
                print(f"Step {self.step_id} failed because upstream step {e.step.step_id} failed too")
                raise e
            except EmptyOutputException as e:
                print(f"STEP: OUTPUT IS EMPTY")
                # don't save as completed but just remove from cache
                cache.append((self, "FAILED", True))
                cache.remove((self, "RUNNING", True))
                if RUN_MODE_DASHBOARD:
                    print(f"""!!!!!! EMPTY OUTPUT !!!!!!!!!""")
                    raise EmptyOutputException(self)
                else:
                    print(f"""!!!!!! STEP FAILED !!!!!!!!!
Step ID: {self.step_id}
Traceback:
{traceback.format_exc()}
Step config {self.__repr__()}""")
                    raise FailedException(self)
            except Exception as e:
                cache.append((self, "FAILED", True))
                cache.remove((self, "RUNNING", True))
                print(f"""!!!!!! STEP FAILED !!!!!!!!!
Step ID: {self.step_id}
Traceback:
{traceback.format_exc()}
Step config {self.__repr__()}""")
                raise FailedException(self)
            if type(output) is dict:
                self.output_tables = output
            else:
                self.output_tables = {self.key: output}
            self.time_completed = datetime.datetime.now()
            cache.append((self, "COMPLETED", True))
            cache.remove((self, "RUNNING", True))
        try:
            self.is_comp_via_exec = True
            return self.output_tables[self.key]
        except KeyError:
            raise StepError(f"Must reference one of the following output tables: {list(self.output_tables.keys())}")

    def __getitem__(self, key):
        ret = copy.deepcopy(self)
        ret.key = key
        return ret

    def __neg__(self):
        ret = copy.deepcopy(self)
        ret.test = True
        return ret

    def __pos__(self):
        ret = copy.deepcopy(self)
        ret.test = False
        return ret

    def _execute(self, cache, debug, test):
        raise NotImplementedError()

    def get_parent_step(self):
        raise NotImplementedError()

    def update_parent_step(self, new_parent, key=None):
        raise NotImplementedError()

    def is_equal(self, other, cache, debug, test) -> bool:
        raise NotImplementedError()

    def in_lineage(self, other) -> bool:
        if self == other:
            return True
        parents = other.get_parent_step()
        if parents is None:
            return False
        elif type(parents) == dict:
            for _, v in parents.items():
                if self.in_lineage(v):
                    return True
            return False
        elif isinstance(parents, Step):
            return self.in_lineage(parents)
        else:
            raise RuntimeError(f"Other's parents are neither step nor dict {parents}")


    def compare_via_execution(self, other, cache, debug, test):
        return "DISTINCT"
        # Check that current steps are comparable via execution
        if debug or not (self.is_comp_via_exec and other.is_comp_via_exec):
            return "DISTINCT"

        # Check that all parent steps are comparable via execution
        if not (self._are_parents_cve() and other._are_parents_cve()):
            return "DISTINCT"

        # Execute and compare
        t1 = self.execute(cache, debug, test)
        t2 = other.execute(cache, debug, test)
        return False

    def _are_parents_cve(self):
        parent_step = self.get_parent_step()
        if parent_step is None:
            return True
        if type(parent_step) == dict:
            for v in parent_step.values():
                if not v.is_comp_via_exec:
                    return False
                if not v._are_parents_cve():
                    return False
            return True
        if not parent_step.is_comp_via_exec:
            return False
        return parent_step._are_parents_cve()

    def compare_parent_keys(self, other, ignore=[]) -> bool:
        if not isinstance(other, Step):
            return False
        if not isinstance(ignore, list):
            raise Exception("ignore keys has to be a list!")
        my_par = self.get_parent_step()
        other_par = other.get_parent_step()
        if my_par == other_par:
            return True
        if type(my_par) != type(other_par):
            return False
        if type(my_par) == dict:
            if set([k for k in my_par.keys() if k not in ignore]) != set([k for k in other_par.keys() if k not in ignore]):
                return False
            for k in my_par.keys():
                if k not in ignore:
                    if my_par[k].key != other_par[k].key:
                        return False
            return True
        return my_par.key == other_par.key

    # For non-equal mergers
    # def is_derivable(self, other, cache, debug, test) -> bool:
    #     raise NotImplementedError()

    def promote_(self, debug, test):
        self.test = test

        try:
            output = self._promote(debug, test)
            print(f"""
Step ID {self.step_id} ({self.__class__.__name__}): Execution complete
""")
        except Exception as e:
            print(f"""!!!!!! STEP FAILED !!!!!!!!!
Step ID: {self.step_id}
Traceback:
{traceback.format_exc()}
Step config {self.__repr__()}""")
            raise FailedException(self)
        self.output_tables = {self.key: output}

    def _promote(self, debug, test):
        raise NotImplementedError

    def purge(self):
        n = datetime.datetime.now()
        if self.time_completed and (n - self.time_completed).days > 0:
            self.remove_self = True
            for t in self.output_tables:
                execute(f"drop table if exists {t}")
