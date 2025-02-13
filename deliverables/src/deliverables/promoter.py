from threading import Thread
import time
import datetime
import traceback
from .steps.step import Step, FailedException
from .utils.runner_utils import update_status, record_failed_steps
from .utils.common import DagType


class DeliveryPromoter:
    def __init__(self, run_id=None):
        self.run_id = run_id

    def run(self, deliveries, nb_max_thread=None, debug=False, test=False, custom=False, optimization_only=False,
            daily_batchtime: str = None):
        if optimization_only:
            debug=True
        print("""
        
======================================================================
====================== PROMOTING DELIVERIES ==========================
======================================================================

""")
        if nb_max_thread is None:
            nb_max_thread = 20 if DagType.MONTHLY.value in self.run_id else 6
        max_threads = nb_max_thread
        for delivery in deliveries:
            delivery.daily_batchtime = daily_batchtime
        delivery_threads = [DeliveryThread(delivery, debug, test, run_id=self.run_id) for delivery in deliveries]
        queued_count = len(delivery_threads)
        running_count = 0
        running_set = {}
        completed_count = 0
        failed_count = 0
        while queued_count > 0 or running_count > 0:
            queue_started = False
            if queued_count > 0 and running_count < max_threads:
                for dt in delivery_threads:
                    if dt.status == "QUEUED":
                        dt.start()
                        queue_started = True
                        break
            if not queue_started:
                print(f"""
                
    ===== Delivery Status Update =====
    time:                 {datetime.datetime.now()}
    Running Deliveries:   {running_count}
    Queued Deliveries:    {queued_count}
    Completed Deliveries: {completed_count}
    Failed Deliveries:    {failed_count}
    Currently running:
    """)
                for d in running_set:
                    print(f"\t", running_set[d])
                time.sleep(1 if debug else 30)

            # Update Delivery Status Counts
            queued_count = 0
            running_count = 0
            completed_count = 0
            failed_count = 0
            for i, dt in enumerate(delivery_threads):
                if dt.status == "QUEUED":
                    queued_count += 1
                elif dt.status == "RUNNING":
                    running_count += 1
                    running_set[dt.delivery.step_id] = dt.delivery
                elif dt.status == "COMPLETED":
                    completed_count += 1
                    dt.join()
                    running_set.pop(dt.delivery.step_id, None)
                elif dt.status == "FAILED":
                    failed_count += 1
                    dt.join()
                    running_set.pop(dt.delivery.step_id, None)
                else:
                    raise RuntimeError(f"Delivery Thread status {dt.status} not recognized")

        print("""
        
==================== EXECUTION COMPLETE!

""")
        print(f"Completed Deliveries: {completed_count}")
        for dt in delivery_threads:
            if dt.status == "COMPLETED":
                print(f"\t {dt.output} | Delivery:", dt.delivery)
        if failed_count != 0:
            print(f"{failed_count} deliveries failed :(")
            for dt in delivery_threads:
                if dt.status == "FAILED":
                    print(f"\t {dt.output} | Delivery:", dt.delivery)
            return False
        else:
            print("""
            
======================================================================
============================= SUCCESS! ===============================
======================================================================

""")
            return True


class DeliveryThread(Thread):
    def __init__(self, delivery: Step, debug, test, run_id:str=None):
        super().__init__()
        self.delivery = delivery
        self.debug = debug
        self.test = test
        self.status = "QUEUED"
        self.run_id = run_id
        # write anytime other than custom run
        self.record_status = ((not test) and (not debug) and (
            DagType.MONTHLY.value in run_id)) or ((not debug) and DagType.DASHBOARD.value in run_id)
        if self.record_status:
            update_status(self.delivery, 'queued for promotion', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id)

        self.output = None
        self.step_output = None

    def run(self) -> None:
        self.status = "RUNNING"
        if self.record_status:
            update_status(self.delivery, 'promoting', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id)
        try:
            self.step_output = self.delivery.promote(self.debug, self.test)
            self.output = f"Step ID: {self.delivery.step_id}"
        except FailedException as e:
            self.output = f"Failed Step ID: {e.step.step_id} | Delivery Step ID: {self.delivery.step_id}"
            self.status = "FAILED"
            tb_str = ''.join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__, limit=2)).replace("'",
                                                                                                                   "")
            comments = self.output + '\n' + tb_str
            if self.record_status:
                update_status(self.delivery, 'promotion failed', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id,
                          comments=comments)
                record_failed_steps(self.delivery, 'promotion failed', test=self.test,
                                    run_id=self.run_id, failed_step_id=e.step.step_id,
                                    step_id=self.delivery.step_id, traceback=tb_str)
        except Exception as e:
            raise e
        else:
            self.status = "COMPLETED"
            comments = self.step_output
            if self.record_status:
                update_status(self.delivery, 'promoted', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id,
                          comments=comments)
