import time

from pvgrip.utils.redis.dictionary \
    import Redis_Dictionary

from pvgrip.globals \
    import PVGRIP_CONFIGS


def test_one():
    d = Redis_Dictionary(name = 'test_one',
                         host = PVGRIP_CONFIGS['redis']['ip'],
                         port = 6379,
                         db = 0)

    assert 'one' not in d
    d['one'] = 1
    assert 'one' in d
    assert d['one'] == 1
    d['one'] = 2
    assert d['one'] == 2
    del d['one']
    assert 'one' not in d

    d[(1.2,'two')] = 2
    assert (1.2,'two') in d
    del d[(1.2,'two')]


def test_timeout():
    d = Redis_Dictionary(name = 'test_timeout',
                         host = PVGRIP_CONFIGS['redis']['ip'],
                         port = 6379,
                         db = 0,
                         expire_time = 5)

    d['one'] = 1
    assert 'one' in d
    time.sleep(6)
    assert 'one' not in d
