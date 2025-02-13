from __future__ import annotations
import random
import string
from enum import unique, Enum
from .sql import execute, load_df
from ..constants import SCHEMA_TEST, SCHEMA_PROD
import datetime
from dateutil.relativedelta import relativedelta


def get_run_id(name:str):
    from datetime import datetime

    dt = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"{name}" if DagType.DASHBOARD.value in name else f"deliverable_{name}_{dt}"
    return run_id


def random_string():
    tbl = 'a' + ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
    return tbl


def compare_input_tables(input_1, input_2, cmv_col, fully_included=False, coverage=False, return_bool=True, test=True):
    with_q = f"""
                with 
                input_table as (
                select
                    company,
                    bitxor_agg(hash({cmv_col})) as company_hash
                from {input_1}
                group by 1),
                other_input_table as (
                select
                    company,
                    bitxor_agg(hash({cmv_col})) as company_hash
                from {input_2}
                group by 1)
    """
    if return_bool:
        select_q = f""" 
        select 
            bool{"or" if coverage else "and"}_agg(
                other_input_table.company_hash = coalesce(input_table.company_hash, other_input_table.company_hash + 1)
            ) as result
        from other_input_table {"left " if fully_included else ""}join input_table using(company);"""
        result = load_df(with_q + select_q)["result"][0]
        return result

    else:
        schema = SCHEMA_TEST if test else SCHEMA_PROD
        tbl = f"{schema}.list_companies_{random_string()}"
        create_q = f"""
        create table {tbl} (company varchar(2048));
        insert into {tbl}
        """
        select_q = f"""
        select distinct company
        from other_input_table join input_table
        using(company)
        where other_input_table.company_hash = coalesce(input_table.company_hash, other_input_table.company_hash + 1);
        """
        execute(create_q + with_q + select_q)
        return tbl


@unique
class DagType(Enum):
    MONTHLY = "monthly"
    CUSTOM = "custom"
    DASHBOARD = "dashboard"

RUN_STATUS_LIST = ["COMPLETED", "FAILED", "WAITING", "WAITING/EXCEPTION", "QUEUED", "RUNNING", "EMPTY"]


def batchtime_diff(batchtime: str, diff: int):
    return (relativedelta(months=diff) + datetime.datetime.strptime(batchtime, '%Y%m')).strftime('%Y%m%d')[:6]