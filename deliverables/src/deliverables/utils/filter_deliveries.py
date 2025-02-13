from ..steps.step import Step
from ..steps.table import *
from ..clients import PackagedDelivery
from ..steps.transform import *
from ..steps.delivery import *
from typing import Callable
import sys


def filter_deliveries_by_step_type(deliveries: list[PackagedDelivery], step_types: list[str]):
    st = [getattr(sys.modules[__name__], x) for x in step_types]
    print(f"Filter step types: {st}")
    filtered = []
    check_condition = lambda x: any([isinstance(x, s) for s in st])
    for d in deliveries:
        if check_tree(d.delivery, check_condition):
            filtered.append(d)
    return filtered


def filter_deliveries_by_input_table(deliveries: list[PackagedDelivery], input_tables: list[str],
                                     partial_table_name: bool = False):
    its = [x.lower() for x in input_tables]
    print(f"Filter input tables: {its}")
    filtered = []
    if partial_table_name:
        check_condition = lambda x: isinstance(x, Table) and any([i.lower() in x.prod_name.lower() or i.lower() in x.test_name.lower() for i in its])
    else:
        check_condition = lambda x: isinstance(x, Table) and any([x.prod_name.lower() == i.lower() or x.test_name.lower() == i.lower() for i in its])

    for d in deliveries:
        if check_tree(d.delivery, check_condition):
            filtered.append(d)
    return filtered


def filter_deliveries_custom(deliveries: list[PackagedDelivery], check_condition: Callable,
                             name: str):
    print(f"Filter custom condition {name}")
    filtered = []
    for d in deliveries:
        if check_tree(d.delivery, check_condition):
            filtered.append(d)
    return filtered


def check_tree(step: Step, check_condition: Callable):
    if check_condition(step):
        return True
    par = step.get_parent_step()
    if par is None:
        return False
    elif type(par) == dict:
        for p in par.values():
            if check_tree(p, check_condition):
                return True
        return False
    else:
        if check_tree(par, check_condition):
            return True
        return False
