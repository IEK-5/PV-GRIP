import time

from pvgrip.utils.tasks \
    import task_test_queueonce


def test_queueonce():
    from pvgrip.utils.exceptions import TASK_RUNNING

    a = task_test_queueonce.delay(sleep = 2, dummy = 1)
    b = task_test_queueonce.delay(sleep = 1, dummy = 1)
    c = task_test_queueonce.delay(sleep = 2, dummy = 1)

    time.sleep(5)
    assert 'FAILURE' == c.state
    assert isinstance(c.result, TASK_RUNNING)
    assert 'SUCCESS' == a.state
    assert a.result
    assert 'SUCCESS' == b.state
    assert b.result
