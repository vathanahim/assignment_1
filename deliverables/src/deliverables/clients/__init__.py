import importlib
from os.path import dirname, basename, isfile, join
import glob
from datetime import date, datetime
from ..constants import Frequencies
from ..steps.transform import CustomTransformStep
from ..steps.delivery import CopySQL
from ..steps.step import Step
from typing import Callable

class PackagedDelivery:
    def __init__(self, delivery, client, client_total, index, frequency, delivery_day, owner, dbs, buckets,
                 sql_retention, sql_cleanup_exceptions, s3_retention, s3_cleanup_exceptions):
        self.delivery = delivery
        self.delivery.delivery_client = client
        self.delivery.delivery_id = index
        self.delivery.delivery_client_total = client_total
        self.client = client
        self.index = index
        self.frequency = frequency
        self.delivery_day = delivery_day
        self.owner = owner
        self.dbs = dbs
        self.buckets = buckets
        self.delivery.sql_retention = sql_retention
        self.delivery.sql_cleanup_exceptions = sql_cleanup_exceptions
        self.delivery.s3_retention = s3_retention
        self.delivery.s3_cleanup_exceptions = s3_cleanup_exceptions

    def __repr__(self):
        return f"({self.client}, {self.index}, {self.delivery})"


def package_deliveries(client):
    i = importlib.import_module(f".{client}", __name__)
    deli_day = None
    if client.lower().endswith('_daily'):
        freq = Frequencies.DAY
    elif client.lower().endswith('_weekly'):
        # weekly deliveries can happen on multiple days of the week
        freq = Frequencies.WEEK
        # we only care about the day weekly delivery happens
        deli_day = i.deli_day
    else:
        freq = Frequencies.MONTH
    dbs = []
    buckets = []

    if hasattr(i, 'CLIENT_BUCKET'):
        buckets.append(i.CLIENT_BUCKET)

    owner = getattr(i, 'OWNER', None)
    sql_retention = getattr(i, 'SQL_RETENTION', None)
    sql_cleanup_exceptions = getattr(i, 'SQL_CLEANUP_EXCEPTIONS', [])
    s3_retention = getattr(i, 'S3_LIVE_RETENTION', None)
    s3_cleanup_exceptions = getattr(i, 'S3_CLEANUP_EXCEPTIONS', [])

    return [PackagedDelivery(d, client, len(i.deliveries), ind, freq, deli_day, owner, dbs, buckets,
                             sql_retention, sql_cleanup_exceptions, s3_retention, s3_cleanup_exceptions) for ind, d in enumerate(i.deliveries)]

def package_post_deliveries(client):
    i = importlib.import_module(f".{client}", __name__)
    if not hasattr(i, 'post_deliveries'):
        return []

    deli_day = None
    if client.lower().endswith('_daily'):
        freq = Frequencies.DAY
    elif client.lower().endswith('_weekly'):
        # weekly deliveries can happen on multiple days of the week
        freq = Frequencies.WEEK
        # we only care about the day weekly delivery happens
        deli_day = i.deli_day
    else:
        freq = Frequencies.MONTH
    owner = getattr(i, 'OWNER', None)
    sql_retention = getattr(i, 'SQL_RETENTION', None)
    sql_cleanup_exceptions = getattr(i, 'SQL_CLEANUP_EXCEPTIONS', [])
    s3_retention = getattr(i, 'S3_LIVE_RETENTION', None)
    s3_cleanup_exceptions = getattr(i, 'S3_CLEANUP_EXCEPTIONS', [])

    return [PackagedDelivery(d, client, len(i.post_deliveries), ind, freq, deli_day, owner, [], [], sql_retention, sql_cleanup_exceptions,
                             s3_retention, s3_cleanup_exceptions) for ind, d in enumerate(i.post_deliveries)]


def package_custom_transforms(client):
    i = importlib.import_module(f".{client}", __name__)
    ret = []

    def check_tree(step: Step, check_condition: Callable):
        if check_condition(step):
            ret.append(step)
        par = step.get_parent_step()
        if par is None:
            return
        elif type(par) == dict:
            for p in par.values():
                check_tree(p, check_condition)
            return
        else:
            check_tree(par, check_condition)
            return

    for d in i.deliveries:
        check_tree(d, lambda x: isinstance(x, CustomTransformStep))
    return ret


# get the name of every python file in this directory
# and pull the client names from the file names
modules = glob.glob(join(dirname(__file__), "*.py"))
ALL_CLIENTS = [basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]
ALL_CLIENTS_PATH = [f for f in modules if isfile(f) and not f.endswith('__init__.py')]

ALL_DELIVERIES = []
for client_ in ALL_CLIENTS:
    ALL_DELIVERIES += package_deliveries(client_)

ALL_CUSTOM_TRANSFORMS = []
ALL_USED_CUSTOM_TRANSFORMS = []
for client_ in ALL_CLIENTS:
    ALL_CUSTOM_TRANSFORMS += package_custom_transforms(client_)
    if not (client_.startswith('test') or client_.startswith('mock') or client_ == 'template'):
        ALL_USED_CUSTOM_TRANSFORMS += package_custom_transforms(client_)

ALL_POST_DELIVERIES = []
for client_ in ALL_CLIENTS:
    ALL_POST_DELIVERIES += package_post_deliveries(client_)
