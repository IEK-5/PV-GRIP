import time
import pytest

from celery.exceptions import \
    TimeoutError

from open_elevation.celery_tasks import\
    task_test_no_nested_celery, \
    task_test_queueonce


def test_no_nested_celery():
    assert False == task_test_no_nested_celery()
    task = task_test_no_nested_celery.delay()
    try:
        task.get(timeout = 2)
        assert True == task.result
    except TimeoutError as e:
        print("Is celery worker running?")
        raise e


def test_queueonce():
    from open_elevation.utils import TASK_RUNNING

    a = task_test_queueonce.delay(sleep = 2, dummy = 1)
    b = task_test_queueonce.delay(sleep = 1, dummy = 1)
    c = task_test_queueonce.delay(sleep = 2, dummy = 1)

    time.sleep(2)
    assert 'FAILURE' == c.state
    assert isinstance(c.result, TASK_RUNNING)
    assert 'SUCCESS' == a.state
    assert a.result
    assert 'SUCCESS' == b.state
    assert b.result
