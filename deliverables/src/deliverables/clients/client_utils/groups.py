from ...steps.table import Table
from ...utils.filter_deliveries import filter_deliveries_by_step_type, filter_deliveries_by_input_table, \
    filter_deliveries_custom
from .. import PackagedDelivery


def filter_deliveries_group(deliveries: list[PackagedDelivery], group: str):
    raise Exception(f"Group {group} is invalid")
