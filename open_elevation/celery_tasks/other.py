import open_elevation.utils
from open_elevation.celery_tasks.app \
    import CELERY_APP


@CELERY_APP.task(bind = True)
def task_test_no_nested_celery():
    if open_elevation.utils.if_in_celery():
        return CELERY_APP.current_worker_task.id
    return False
