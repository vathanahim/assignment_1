from threading import Thread
import time
import datetime
import traceback
from .steps.step import Step, WaitingException, FailedException, EmptyOutputException
from .utils.runner_utils import update_status, record_failed_steps
from .utils.common import DagType, RUN_STATUS_LIST

class DeliveryRunner:
    def __init__(self, run_id=None,  metadata=None,
                 use_dev_pipeline_service=False, custom_pipeline_image=None, data_version="current",
                 data_recency="monthly", failed_status:list[str]=None, completed_status:list[str]=None,
                 daily_batchtime: str = None):
        self.run_id = run_id
        self.cache = []
        self.steps = []
        self.ts_steps = []
        self.wfd_steps = []
        self.metadata = metadata
        self.use_dev_pipeline_service = use_dev_pipeline_service
        self.custom_pipeline_image = custom_pipeline_image
        self.daily_batchtime = daily_batchtime
        self.data_version = data_version
        self.data_recency = data_recency
        self.failed_status = ["FAILED"] if not failed_status else failed_status
        self.completed_status = ["COMPLETED"] if not completed_status else completed_status

    def add_step(self, step, debug, test, custom, is_newly_added=False):
        try:
            step.data_version = self.data_version
            step.daily_batchtime = self.daily_batchtime
            self.steps.append(step)

            parent_step = step.get_parent_step()
            if parent_step is None:
                return
            if type(parent_step) == dict:
                for k, v in parent_step.items():
                    for dag_step in self.steps:
                        if v.key == dag_step.key and v.is_equal(dag_step, self.cache, debug, test):
                            if not step.in_lineage(dag_step):
                                step.update_parent_step(dag_step, k)
                            break
                    else:
                        self.add_step(v, debug, test, custom, is_newly_added=True)
                return

            for dag_step in self.steps:
                if parent_step.key == dag_step.key and parent_step.is_equal(dag_step, self.cache, debug, test):
                    if not step.in_lineage(dag_step):
                        step.update_parent_step(dag_step)
                        return
            self.add_step(parent_step, debug, test, custom, is_newly_added=True)
        except Exception as e:
            print(f"\nOptimization failed with error:\n{e}")
            print(f"\nOptimization failed while adding step:\n{step.__repr__()}")
            raise RuntimeError()


    def run(self, deliveries, nb_max_thread=None, debug=False, test=False, custom=False, optimization_only=False,
            auto_cleanup=True):
        try:
            # Optimization
            print("""
        
======================================================================
=================== PART 1: OPTIMIZING DELIVERIES ====================
======================================================================

""")
            deliveries_added = 0
            cached_deliveries = 0
            new_deliveries = []
            time_saved = datetime.datetime.now()

            for delivery in deliveries:
                time_now = datetime.datetime.now()
                print(f"""
-------- Delivery Optimization Checkpoint
time:             {time_now}
deliveries added: {deliveries_added}
cached deliveries:{cached_deliveries}
steps size:       {len(self.steps)}
cache size:       {len(self.cache)}
ts step size:     {len(self.ts_steps)}
""")
                if (time_now - time_saved).seconds > 300:
                    print("WARNING! JUMP IN TIME DETECTED!")
                time_saved = time_now

                # Check if the step is already in the cache
                for step, status, is_newly_added in self.cache:
                    if str(step) == str(delivery) and step.is_equal(delivery, self.cache, debug, test):
                        cached_deliveries += 1
                        break
                else:
                    new_deliveries.append(delivery)
                    self.add_step(delivery, debug, test, custom, is_newly_added=True)
                    deliveries_added += 1
        except:
            print(f"""
        
==================== OPTIMIZATION FAILED!
Traceback:
{traceback.format_exc()}
""")
            return False, [], (self.cache, self.cache_key), None

        else:
            print(f"""
            
==================== OPTIMIZATION COMPLETE!
deliveries added: {deliveries_added}
cached deliveries:{cached_deliveries}
steps size:       {len(self.steps)}
cache size:       {len(self.cache)}
ts step size:     {len(self.ts_steps)}

""")

        print("""
        
======================================================================
==================== PART 2: EXECUTING DELIVERIES ====================
======================================================================

""")
        last_save = time.time()

        if optimization_only:
            debug = True

        if nb_max_thread is None: 
            nb_max_thread = 20 if DagType.MONTHLY.value in self.run_id else 6
        max_threads = nb_max_thread
        delivery_threads = [DeliveryThread(delivery, self.cache, debug, test, run_id=self.run_id) for delivery in new_deliveries]
        emptied = []
        running_set = {}
        counts = {c: 0 for c in RUN_STATUS_LIST if c not in ["QUEUED"]}
        counts["QUEUED"] = len(delivery_threads)
        while counts["QUEUED"] + counts["WAITING"] > 0 or counts["RUNNING"] > 0:
            waiting_started = False
            queue_started = False
            if counts["QUEUED"] + counts["WAITING"] > 0 and counts["RUNNING"] < max_threads:
                for dt in delivery_threads:
                    if dt.status == "WAITING":
                        in_cache = False
                        for step, status, is_newly_added in self.cache:
                            if step == dt.waiting_step:
                                in_cache = True
                                if status != "RUNNING":
                                    dt.start()
                                    waiting_started = True
                                    print(f"reattempt {dt.delivery} after step gained status {status}")
                                    break
                        if not in_cache:
                            dt.start()
                            waiting_started = True
                            print(f"reattempt {dt.delivery} after cached step was lost")
                            print(f"lost step: {dt.waiting_step}")

                    if waiting_started:
                        break
                    else:
                        if dt.status == "QUEUED":
                            dt.start()
                            queue_started = True
                            break

            if not waiting_started and not queue_started:
                if not debug and time.time() - last_save > 600:
                    last_save = time.time()
                print(f"""
                
===== Delivery Status Update =====
time:                 {datetime.datetime.now()}
Running Deliveries:   {counts["RUNNING"]}
Queued Deliveries:    {counts["QUEUED"]}
Completed Deliveries: {counts["COMPLETED"]}
Waiting Deliveries:   {counts["WAITING"]}
Failed Deliveries:    {counts["FAILED"]}
Empty Deliveries:     {counts["EMPTY"]}
Currently running:
""")
                for d in running_set:
                    print(f"\t", running_set[d])
                time.sleep(1 if debug else 30)

            # Update Delivery Status Counts
            counts = {c: 0 for c in RUN_STATUS_LIST}

            for i, dt in enumerate(delivery_threads):
                if dt.status == "QUEUED":
                    counts["QUEUED"] += 1
                elif dt.status == "RUNNING":
                    counts["RUNNING"] += 1
                    running_set[dt.delivery.step_id] = dt.delivery
                elif dt.status == "WAITING/EXCEPTION":
                    counts["WAITING"] += 1
                    tmp_ws = dt.waiting_step
                    if dt.is_alive():
                        dt.join()
                    dt = DeliveryThread(new_deliveries[i], self.cache, debug, test, run_id=self.run_id)
                    delivery_threads[i] = dt
                    dt.status = "WAITING"
                    dt.waiting_step = tmp_ws
                    running_set.pop(dt.delivery.step_id, None)
                elif dt.status == "WAITING":
                    counts["WAITING"] += 1
                    running_set.pop(dt.delivery.step_id, None)
                elif dt.status == "COMPLETED":
                    counts["COMPLETED"] += 1
                    dt.join()
                    running_set.pop(dt.delivery.step_id, None)
                elif dt.status == "EMPTY":
                    counts["EMPTY"] += 1
                    emptied.append(dt.delivery)
                    dt.join()
                    running_set.pop(dt.delivery.step_id, None)
                elif dt.status == "FAILED":
                    counts["FAILED"] += 1
                    dt.join()
                    running_set.pop(dt.delivery.step_id, None)
                else:
                    raise RuntimeError(f"Delivery Thread status {dt.status} not recognized")
        print("""
        
==================== EXECUTION COMPLETE!

""")
        print(f"Completed Deliveries: {counts['COMPLETED']}{', Empty Deliveries: ' + str(counts['EMPTY']) if DagType.DASHBOARD.value in self.run_id else ''}")
        completed = []
        for dt in delivery_threads:
            if dt.status in self.completed_status:
                print(f"\t {dt.output} | Delivery:", dt.delivery)
                if dt.status != "EMPTY":
                    completed.append(dt.delivery)
        failed_f = sum(counts[c] for c in self.failed_status)
        if failed_f != 0:
            print(f"{failed_f} deliveries failed :(")
            for dt in delivery_threads:
                if dt.status in self.failed_status:
                    print(f"\t {dt.output} | Delivery:", dt.delivery)

        if failed_f != 0 or ("EMPTY" in self.completed_status and len(deliveries) == counts["EMPTY"]):
            return False, completed
        else:
            print("""

            ======================================================================
            ============================= SUCCESS! ===============================
            ======================================================================

            """)
            return True, completed


class DeliveryThread(Thread):
    def __init__(self, delivery: Step, cache, debug, test, run_id:str=None):
        super().__init__()
        self.delivery = delivery
        self.cache = cache
        self.debug = debug
        self.test = test
        self.status = "QUEUED"
        self.run_id = run_id
        # write anytime other than custom run
        self.record_status = ((not test) and (not debug) and (
            DagType.MONTHLY.value in run_id)) or ((not debug) and DagType.DASHBOARD.value in run_id)
        if self.record_status:
            update_status(self.delivery, 'queued', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id)

        self.output = None
        self.waiting_step = None
        self.step_output = None


    def run(self) -> None:
        self.status = "RUNNING"
        if self.record_status:
            update_status(self.delivery, 'running', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id)
        try:
            self.step_output = self.delivery.execute(self.cache, self.debug, self.test)
            self.output = f"Step ID: {self.delivery.step_id}"
        except EmptyOutputException as e:
            print(f"RUNNER: OUTPUT IS EMPTY")
            if DagType.DASHBOARD.value in self.run_id:
                self.output = f"EMPTY Step ID: {e.step.step_id} | Delivery Step ID: {self.delivery.step_id}"
                self.status = "EMPTY"
                tb_str = ''.join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__, limit=2)).replace(
                    "'",
                    "")
                comments = self.output + '\n' + tb_str
                if self.record_status:
                    update_status(self.delivery, 'empty', test=self.test, run_id=self.run_id,
                                  step_id=self.delivery.step_id,
                                  comments=comments)
                    record_failed_steps(self.delivery, 'empty', test=self.test,
                                        run_id=self.run_id, failed_step_id=e.step.step_id,
                                        step_id=self.delivery.step_id, traceback=tb_str)
            else:
                raise e
        except WaitingException as e:
            self.status = "WAITING/EXCEPTION"
            self.waiting_step = e.step
        except FailedException as e:
            self.output = f"Failed Step ID: {e.step.step_id} | Delivery Step ID: {self.delivery.step_id}"
            self.status = "FAILED"
            tb_str = ''.join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__, limit=2)).replace("'",
                                                                                                                   "")
            comments = self.output + '\n' + tb_str
            if self.record_status:
                update_status(self.delivery, 'failed', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id,
                          comments=comments)
                record_failed_steps(self.delivery, 'failed', test=self.test,
                                    run_id=self.run_id, failed_step_id=e.step.step_id,
                                    step_id=self.delivery.step_id, traceback=tb_str)
        except Exception as e:
            raise e
        else:
            self.status = "COMPLETED"
            comments = self.step_output + ("" if not self.delivery.qa else f" (QA: {self.delivery.qa})")
            if self.record_status:
                update_status(self.delivery, 'completed', test=self.test, run_id=self.run_id, step_id=self.delivery.step_id,
                          comments=comments)
