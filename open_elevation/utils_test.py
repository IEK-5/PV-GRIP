from celery.exceptions import \
    TimeoutError

from open_elevation.celery_tasks import\
    task_test_no_nested_celery


def test_no_nested_celery():
    assert False == task_test_no_nested_celery()
    task = task_test_no_nested_celery.delay()
    try:
        task.get(timeout = 2)
        assert True == task.result
    except TimeoutError as e:
        print("Is celery worker running?")
        raise e
