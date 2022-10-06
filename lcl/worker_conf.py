from celery import Celery, bootsteps
from click import Option


def _setup_command_options(app: Celery):
    app.user_options["worker"].add(
        Option(
            ("--is-one-task-executer",),
            is_flag=True,
            help="Worker use for execute one task"
        )
    )


class TimeoutBootstep(bootsteps.Step):
    def __init__(self, parent, is_one_task_executer: bool = False, **options):
        super().__init__(parent, **options)

        if is_one_task_executer:
            self.register_timeout_checker()

    def register_timeout_checker(self):
        pass  # TODO


def _setup_bootsteps(app: Celery):
    app.steps["worker"].add(TimeoutBootstep)


def setup(app: Celery):
    _setup_command_options(app)
    _setup_bootsteps(app)
