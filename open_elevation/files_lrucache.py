import os
import time
import diskcache


class Files_LRUCache:


    def __init__(self, maxsize, path = '.', check_every = 6):
        """Implements LRU list of file paths

        :maxsize: maximum size in GB of stored files

        :path: path where to store diskcache db

        :check_every: number of hours to check the content of cache

        """
        self.maxsize = maxsize*(1024**3)
        self.check_every = check_every * (60**2)

        self.path = path
        os.makedirs(self.path, exist_ok = True)

        path = os.path.join(self.path,"_Files_LRUCache")
        self._deque = diskcache.Deque\
            (directory = path + '_deque')
        self._sizes = diskcache.Cache\
            (directory = path + '_cache',
             size_limit = 100*(1024**2))
        self._lock = diskcache.RLock(self._sizes, '_lock')

        with self._lock:
            if 'total' not in self._sizes:
                self._sizes['total'] = 0
            if 'checked_at' not in self._sizes:
                self._sizes['checked_at'] = time.time()


    def _remove_from_sizes(self, item):
        if ('file://' + item) in self._sizes:
            self._sizes['total'] -= \
                self._sizes['file://' + item]
            del self._sizes['file://' + item]
            return True

        return False


    def check_content(self):
        """Check content of lists and remove deleted files
        """
        with self._lock:
            self._sizes['checked_at'] = time.time()
            [p in self for p in self._deque]


    def _update(self, item):
        if item in self._deque:
            self._deque.remove(item)
        self._deque.append(item)

        if not os.path.exists(item):
            if self._remove_from_sizes(item):
                self._deque.remove(item)
            return

        size = os.stat(item).st_size

        if ('file://' + item) not in self._sizes:
            self._sizes['total'] += size
            self._sizes['file://' + item] = size
            return

        if self._sizes['file://' + item] != size:
            self._sizes['total'] += \
                size - self._sizes['file://' + item]
            self._sizes['file://' + item] = size
            return

        if (time.time() - self._sizes['checked_at']) > self.check_every:
            self.check_content()


    def add(self, fn):
        """Add a file to a cache

        File does not have to exist at a time of addition. Any query
        about the file updates information in cache.

        :fn: path to a file

        """
        with self._lock:
            while self._sizes['total'] >= self.maxsize \
                  and len(self._deque) > 0:
                self.popleft()
            self._update(fn)


    def __contains__(self, item):
        if item not in self._deque:
            return False

        with self._lock:
            self._update(item)

        if not os.path.exists(item):
            return False

        return True


    def __len__(self):
        return len(self._deque)


    def size(self):
        """Return total used space in bytes
        """
        return self._sizes['total']


    def popleft(self):
        """Pop the least recently used file

        Popping tries to delete the tracked by cache file.
        """
        with self._lock:
            item = self._deque.popleft()
            try:
                os.remove(item)
            except:
                pass

            self._remove_from_sizes(item)
            return item