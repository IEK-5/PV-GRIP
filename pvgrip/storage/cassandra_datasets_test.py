from pvgrip.globals \
    import PVGRIP_CONFIGS

from pvgrip.storage.cassandra_datasets \
    import Datasets


def test_files(ip = PVGRIP_CONFIGS['cassandra']['ip']):
    try:
        datasets = Datasets(keyspace = 'test_datasets',
                            cluster_ips = [ip])

        assert 0 == len(list(datasets.list()))
        datasets.add('one')
        assert 1 == len(list(datasets.list()))
        datasets.add('two')
        assert 2 == len(list(datasets.list()))
        datasets.add('one')
        assert 2 == len(list(datasets.list()))
    finally:
        try:
            datasets.drop_keyspace()
        except:
            pass
