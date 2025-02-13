from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

RUN_MODE_DASHBOARD = False

class RunModes:
    PROD = 'prod'
    OPTI_ONLY = 'opti_only'
    TEST = 'test'
    DEBUG = 'debug'
    TABLE_CHECK = 'table_check'

class Frequencies:
    MONTH = "monthly"
    WEEK = "weekly"
    DAY = "daily"

class DayOfWeek:
    MONDAY = 'Monday'
    TUESDAY = 'Tuesday'
    WEDNESDAY = 'Wednesday'
    THURSDAY = 'Thursday'
    FRIDAY = 'Friday'
    TODAY = date.today().strftime("%A")

PREV_BATCHTIME = '202412'
BATCHTIME = '202501'
NEXT_BATCHTIME = '202502'
JP_END = '2025-01'
PREV_JP_END = '2024-12'

DAILY_BATCHTIME = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
WEEKLY_BATCHTIME = (datetime.now(ZoneInfo("America/New_York")) + timedelta(days=15)).strftime("%Y%m")
PREV_DAILY_BATCHTIME = (datetime.now(ZoneInfo("America/New_York")) - timedelta(days=1)).strftime("%Y%m%d")
PREV_WEEK_BATCHTIME = (datetime.now(ZoneInfo("America/New_York")) - timedelta(days=7)).strftime("%Y%m%d")
DAILY_BATCHTIME_DATE_ANNA=datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

SCHEMA_TEST = 'TEST_RUN_TABLES'
SCHEMA_TEST_PROMOTE = 'TEST_PROMOTE_TABLES'
SCHEMA_PROD = 'PROD_RUN_TABLES'
SCHEMA_PROD_PROMOTE = 'PROD_PROMOTE_TABLES'
SCHEMA_STATUS = 'STATUS'
SCHEMA_STATUS_TEST = 'STATUS_TEST'

