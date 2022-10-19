from datetime import datetime, timedelta, timezone
from numbers import Real
from threading import Thread
from typing import Union, Callable, Optional, Any

from celery import Celery, Task
from celery.app.task import ExceptionInfo
from celery.exceptions import Retry
from celery.signals import after_task_publish


START_WORKER_FUNC_TYPE = Callable[[], None]


def _inner_shutdown(app, hostname):
    app.control.shutdown(destination=[hostname])


class OneTaskExecuterTaskBase(Task):
    START_WORKER_FUNC: Optional[START_WORKER_FUNC_TYPE] = None

    RECREATE_WORKER_ON_RETRY = False
    RECREATE_WORKER_MINIMAL_COUNTDOWN: Optional[timedelta] = timedelta(seconds=60)
    START_WORKER_TASK: Optional[Task] = None

    TASK_QUEUE: Optional[str] = None

    @classmethod
    def start_worker(cls):
        assert cls.START_WORKER_FUNC is not None, (
            "'%s' should either include a `START_WORKER_FUNC` attribute, " % cls.__name__
        )

        cls.START_WORKER_FUNC()

    @classmethod
    def start_worker_delayed(cls, delay: int):
        assert cls.START_WORKER_TASK is not None, (
            "'%s' should either include a `START_WORKER_TASK` attribute, " % cls.__name__
        )

        cls.START_WORKER_TASK.apply_async(countdown=delay)

    @classmethod
    def get_task_queue(cls) -> str:
        assert cls.TASK_QUEUE is not None, "'%s' should either include a `TASK_QUEUE` attribute, " % cls.__name__

        return cls.TASK_QUEUE

    @classmethod
    def on_bound(cls, app: Celery):
        after_task_publish.connect(cls._start_new_worker, sender=cls.name)

    def apply_async(self, *args, **kwargs):
        return super().apply_async(*args, **{**kwargs, "queue": self.TASK_QUEUE})

    def _shutdown(self, hostname: str):
        Thread(target=_inner_shutdown, args=(self.app, hostname)).start()

    @classmethod
    def _start_new_worker(cls, *args, **kwargs):
        retries = kwargs["headers"]["retries"]

        if retries == 0:
            return cls.start_worker()

    def on_success(self, retval: Any, task_id: str, args, kwargs) -> None:
        self._shutdown(self.request.hostname)

    def on_failure(self, exc: Exception, task_id: str, args, kwargs, einfo: ExceptionInfo) -> None:
        self._shutdown(self.request.hostname)

    def on_retry(self, exc: Exception, task_id: str, args, kwargs, einfo: ExceptionInfo) -> None:
        if not self.RECREATE_WORKER_ON_RETRY:
            return

        if self.RECREATE_WORKER_MINIMAL_COUNTDOWN is None:
            self.start_worker()
            self._shutdown(self.request)
            return

        assert isinstance(einfo.exception, Retry)
        retry: Retry = einfo.exception

        when: Union[Real, datetime] = retry.when  # type: ignore

        if isinstance(when, datetime):
            time_delta: int = (when - datetime.now(timezone.utc)).seconds
        else:
            time_delta = int(when)

        if time_delta >= self.RECREATE_WORKER_MINIMAL_COUNTDOWN.seconds:
            self.start_worker_delayed(time_delta)
            self._shutdown(self.request.hostname)
