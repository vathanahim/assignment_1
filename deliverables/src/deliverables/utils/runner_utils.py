from .sql import load_df, execute
import datetime
from ..constants import SCHEMA_STATUS, SCHEMA_STATUS_TEST, BATCHTIME
from .common import DagType


def get_pipeline_name(step_id, run_id, test, ts):
    if run_id:
        id_name = "run_id"
        prefix = "Data Builder"
        id_value = run_id
    else:
        id_name = "step_id"
        prefix = "15th Automation"
        id_value = step_id
    prod_test = " [PROD]" if not test else " [TEST]"
    ts_name = "" if not ts else " (Timescaling)"

    return f"{prefix} Pipeline{ts_name}{prod_test}: {id_name} {id_value}"


def get_status_metadata(test, run_id:str, for_failed:bool=False, for_pipeline:bool=False) -> (str, str):
    prefix = "" if not test else "TEST_"
    schema = SCHEMA_STATUS if not test else SCHEMA_STATUS_TEST
    if DagType.DASHBOARD.value in run_id:
        prefix += "DASHBOARD_"
        join_cond = f"AND run_id='{run_id}' "

    else:
        prefix += f"{DagType.MONTHLY.name}_{BATCHTIME}_"
        join_cond = ''
    failed = "" if not for_failed else "_FAILED"
    pipeline = "" if not for_pipeline else "PIPELINE_"
    status_table = f"{schema}.{prefix}DELIVERABLE_{pipeline}STATUS{failed}"
    return status_table, join_cond


def create_status_table(test, run_id):
    status_table, _ = get_status_metadata(test, run_id)
    query = f"""
    create table if not exists {status_table} (
        RUN_ID VARCHAR(2048),
        STEP_ID VARCHAR(64),
        CLIENT VARCHAR(512),
        CLIENT_TOTAL_DELIVERIES INT,
        DELIVERY VARCHAR(2048),
        DELIVERY_ID INT,
        STATUS VARCHAR(2048),
        UPD_TIME TIMESTAMP(6),
        LATEST BOOLEAN,
        COMMENTS LONGTEXT
    );
    """
    warehouse = '15TH_AUTOMATION_STATUS'
    execute(query)

    failed_status_table, _ = get_status_metadata(test, run_id, for_failed=True)
    query = f"""
    create table if not exists {failed_status_table} (
        RUN_ID VARCHAR(2048),
        STEP_ID VARCHAR(64),
        CLIENT VARCHAR(512),
        CLIENT_TOTAL_DELIVERIES INT,
        DELIVERY VARCHAR(2048),
        DELIVERY_ID INT,
        STATUS VARCHAR(2048),
        UPD_TIME TIMESTAMP(6),
        LATEST BOOLEAN,
        FAILED_STEP_ID VARCHAR(64),
        TRACEBACK LONGTEXT
    );
    """
    execute(query)

def record_job_ids(run_id, job_id, client, name, retries=0, test=False):
    """
    Keep track of job_ids for dashboard deliverables
    prod: service_15th_automation.status.dashboard_deliverable_job_ids
    dev: service_15th_automation.status_test.test_dashboard_deliverable_job_ids
    columns: job_id, run_id, client, name, upd_time
    """
    prefix = "" if not test else "TEST_"
    schema = SCHEMA_STATUS if not test else SCHEMA_STATUS_TEST
    table = f"{schema}.{prefix}DASHBOARD_DELIVERABLE_JOB_IDS"
    warehouse = '15TH_AUTOMATION_STATUS'
    try:
        execute(f"""
            INSERT INTO {table} (run_id, job_id, client, name, created_time)
            SELECT '{run_id}', '{job_id}','{client}', '{name}','{datetime.datetime.now()}';
            """)
    except Exception as e:
        if retries >= 10:
            print(f"Unable to access job id table")
        else:
            record_job_ids(run_id, job_id, client, name, retries=retries + 1, test=test)


def update_job_ids(run_id, job_id, status, retries=0, test=False):
    """
    Record completion time
    """
    prefix = "" if not test else "TEST_"
    schema = SCHEMA_STATUS if not test else SCHEMA_STATUS_TEST
    table = f"{schema}.{prefix}DASHBOARD_DELIVERABLE_JOB_IDS"
    warehouse = '15TH_AUTOMATION_STATUS'

    try:
        execute(f"""
            UPDATE {table}
            SET status = '{status}', upd_time = '{datetime.datetime.now()}'
            WHERE run_id='{run_id}' and job_id='{job_id}';
            """)
    except Exception as e:
        if retries >= 10:
            print(f"Unable to to set status {status} for run_id {run_id}, job_id {job_id}")
        else:
            update_job_ids(run_id, job_id, status, retries=retries + 1, test=test)



def update_status(delivery, status, retries=0, test=False, run_id=None, step_id=None, comments: str = None):
    """
    Write to status table
    - main dag will write to batchtime_deliverable_status (prod only)
    - dashboard dag will write to (test_)dashboard_deliverable_status (test and prod)
    """
    c = delivery.delivery_client
    status_table, where = get_status_metadata(test, run_id)

    comments = 'NULL' if not comments else comments
    warehouse = '15TH_AUTOMATION_STATUS'

    if not (c.startswith('test') or c.startswith('mock') or c == 'template'):
        try:
            execute(f"""
                UPDATE {status_table}
                SET latest=FALSE
                WHERE client='{c}' {where}AND delivery='{delivery}';
                
                INSERT INTO {status_table} (run_id, step_id, client, client_total_deliveries, delivery, delivery_id, status, upd_time, latest, comments)
                SELECT '{run_id}', '{step_id}',  '{c}', {delivery.delivery_client_total}, 
                '{delivery}', {delivery.delivery_id}, '{status}', '{datetime.datetime.now()}', TRUE, '{comments}';
                """)
        except Exception as e:
            if retries >= 10:
                print(f"Unable to set status {status} for delivery {delivery}")
            else:
                update_status(delivery, status, retries=retries+1, test=test, run_id=run_id)


def record_failed_steps(delivery, status: str, test: bool, run_id: str, failed_step_id: str, step_id: str,
                        traceback: str, retries=0):
    tbl, where = get_status_metadata(test, run_id, for_failed=True)
    warehouse = '15TH_AUTOMATION_STATUS'

    try:
        execute(f"""
                UPDATE {tbl}
                SET latest=FALSE
                WHERE client='{delivery.delivery_client}' {where}AND step_id='{step_id}';
                INSERT INTO {tbl} (run_id, step_id, client, client_total_deliveries, delivery, delivery_id, status, upd_time, latest, failed_step_id, traceback)
                SELECT '{run_id}', '{step_id}',  '{delivery.delivery_client}', {delivery.delivery_client_total}, 
                '{delivery}', {delivery.delivery_id}, 'failed', '{datetime.datetime.now()}', TRUE, '{failed_step_id}', '{traceback}';
            """)
    except Exception as e:
        if retries >= 10:
            print(f"Unable to record failed status {status} for delivery {delivery}")
        else:
            record_failed_steps(delivery, status, retries=retries + 1, test=test, run_id=run_id,
                                failed_step_id=failed_step_id, step_id=step_id, traceback=traceback)


def update_pipeline_status_table(job_id, run_id, pipeline_id, status, test=False, retries=0):
    """
    prod: service_15th_automation.status.dashboard_deliverable_pipeline_status
    dev: service_15th_automation.status_test.test_dashboard_deliverable_pipeline_status
    columns: job_id, run_id, pipeline_id, status, latest, upd_time


    select job_id, pipeline_id, status from
    {table} where job_id='{job_id}' AND latest = TRUE order by job_id, pipeline_id;

    """
    status_table, _ = get_status_metadata(test, run_id, for_pipeline=True)
    warehouse = '15TH_AUTOMATION_STATUS'

    try:
        execute(f"""
            UPDATE {status_table}
            SET latest=FALSE
            WHERE job_id='{job_id}' AND pipeline_id='{pipeline_id}';

            INSERT INTO {status_table} (job_id, run_id, pipeline_id, status, upd_time, latest)
            SELECT'{job_id}', '{run_id}',  '{pipeline_id}', '{status}', '{datetime.datetime.now()}', TRUE;
            """)
    except Exception as e:
        if retries >= 10:
            print(f"Unable to set status {status} for pipeline id {pipeline_id}")
        else:
            update_pipeline_status_table(job_id, run_id, pipeline_id, status, retries=retries + 1, test=test)


def get_metadata(metadata):
    if not metadata:
        return {}
    if not all(c in ("user", "run_id", "job_id") for c in metadata.keys()):
        raise ValueError(f"Not all required arguments are passed: {metadata}")
    return metadata
