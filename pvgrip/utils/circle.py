class Circle:
    """Implement a circle

    """
    def __init__(self):
        self._circle = []
        self._i = None


    def __call__(self):
        if 0 == len(self._circle):
            return None

        res = self._circle[self._i]
        self._i += 1
        self._i %= len(self._circle)
        return res


    def add(self, item):
        self._circle.append(item)

        if self._i is None:
            self._i = 0
