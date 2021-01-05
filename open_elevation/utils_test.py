from open_elevation.utils import \
    retry

@retry(max_attempts = 10, sleep_on_task = 0.1)
def job(fail = True):
    if fail:
        raise RuntimeError("job failed")
    return not fail


def test_retry():
    try:
        job(fail = True)
    except:
        pass
