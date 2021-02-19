from open_elevation.cassandra_datasets \
    import Datasets


def test_files(ips = ['172.17.0.2']):
    try:
        datasets = Datasets(keyspace = 'test_datasets',
                            cluster_ips = ips)

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
