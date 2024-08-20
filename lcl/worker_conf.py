from datetime import datetime
from typing import Optional

from celery import Celery, bootsteps
from celery.worker import WorkController


class TimeoutBootstep(bootsteps.StartStopStep):
    requires = {"celery.worker.components:Timer"}

    TIMEOUT_SECONDS = 5 * 60

    def __init__(self, worker, is_one_task_executer: bool = False, **options):
        super().__init__(worker, **options)

        self.t_ref = None

        self.last_processed_task_update_time: Optional[datetime] = None
        self.last_processed_task_count: Optional[int] = None

    def start(self, worker: WorkController):
        self.t_ref = worker.timer.call_repeatedly(30.0, self.check_timeout, (worker,), priority=10)

    def stop(self, worker: WorkController):
        pass

        # if self.t_ref:
        #     self.t_ref.cancel()
        #     self.t_ref = None

    def check_timeout(self, worker: WorkController):
        print("Current active requests:", len(worker.state.active_requests))

        if len(worker.state.active_requests) != 0:
            return

        current_value = worker.state.all_total_count[0]

        if self.last_processed_task_count != current_value:
            self.last_processed_task_count = current_value
            self.last_processed_task_update_time = datetime.now()
            return

        assert self.last_processed_task_update_time is not None

        delta_time = datetime.now() - self.last_processed_task_update_time
        if delta_time.seconds >= self.TIMEOUT_SECONDS:
            raise SystemExit()


def setup_lcl(app: Celery):
    app.steps["worker"].add(TimeoutBootstep)
