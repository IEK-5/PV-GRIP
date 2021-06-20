from cassandra_io.base \
    import Cassandra_Base


class Datasets(Cassandra_Base):
    """Keep track of available datasets

    """

    def __init__(self, **kwargs):
        if 'keyspace' not in kwargs:
            kwargs['keyspace'] = 'cassandra_spatial_datasets'
        super().__init__(**kwargs)

        self._session = self._cluster.connect(self._keyspace)
        queries = self._create_tables_queries()
        for _, query in queries.items():
            self._session.execute(query)
        self._queries.update(queries)
        self._queries.update(self._insert_queries())
        self._queries.update(self._select_queries())
        self._queries.update(self._delete_queries())


    def _create_tables_queries(self):
        res = {}
        res['create'] = """
        CREATE TABLE IF NOT EXISTS
        datasets
        (
        dataset text,
        PRIMARY KEY (dataset))"""
        return res


    def _insert_queries(self):
        res = {}
        res['insert'] = """
        INSERT INTO datasets
        (dataset)
        VALUES (%s)
        IF NOT EXISTS
        """
        return res


    def _select_queries(self):
        res = {}
        res['select'] = \
            self._session.prepare\
            ("""
            SELECT dataset
            FROM datasets""")
        return res


    def _delete_queries(self):
        res = {}
        res['delete'] = \
            self._session.prepare\
            ("""
            DELETE FROM datasets
            WHERE dataset=?
            IF EXISTS""")
        return res


    def add(self, dataset):
        """Add dataset name to the list

        :dataset: string

        """
        self._session.execute\
            (self._queries['insert'],
             (dataset,))


    def list(self):
        """Generate all available dataset names

        """
        for x in self._session.execute\
            (self._queries['select']):
            yield x[0]
