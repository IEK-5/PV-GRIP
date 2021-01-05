from celery.exceptions import \
    TimeoutError

import open_elevation.utils
from open_elevation.celery_tasks import\
    task_test_no_nested_celery


@open_elevation.utils.retry(max_attempts = 10, sleep_on_task = 0.1)
def job(fail = True):
    if fail:
        raise RuntimeError("job failed")
    return not fail


def test_retry():
    try:
        job(fail = True)
    except:
        pass


def test_no_nested_celery():
    assert False == task_test_no_nested_celery()
    task = task_test_no_nested_celery.delay()
    try:
        task.get(timeout = 2)
        assert True == task.result
    except TimeoutError as e:
        print("Is celery worker running?")
        raise e
