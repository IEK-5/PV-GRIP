import signal


class TIMEOUT(Exception):
    pass


class Timeout:


    def __init__(self, timeout = 10):
        self._timeout = timeout


    def _handler(self, signum, frame):
        raise TIMEOUT("something timed out after {} seconds"\
                      .format(self._timeout))


    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handler)
        signal.alarm(self._timeout)
        pass


    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
