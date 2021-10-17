from pvgrip \
    import CELERY_APP
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.webserver.get_task \
    import get_task


@CELERY_APP.task(bind=True)
@one_instance(expire = 3600)
def generate_task_queue(self, method, args):
    """Generate processing queue

    :method, args: see webserver/utils.py::call_method

    :return: the task id of self (prefixed with a string) or raise an
exception on error

    """
    # from here it is narrow
    task = get_task(method = method, args = args)
    job = task(**args).delay()
    return job.task_id
