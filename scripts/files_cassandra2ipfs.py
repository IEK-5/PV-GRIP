from tqdm import tqdm

from pvgrip.globals import \
    get_CASSANDRA_STORAGE, get_IPFS_STORAGE

from pvgrip.storage.remotestorage_path \
    import RemoteStoragePath


def transferdata():
    cfs = get_CASSANDRA_STORAGE()
    ipfs = get_IPFS_STORAGE()

    files = cfs._session.execute\
            (cfs._queries['select_current_filenames'])

    for filename, timestamp in tqdm(files):
        ipfs_fn = RemoteStoragePath(filename, 'ipfs_path')
        cfs_fn = RemoteStoragePath(filename, 'cassandra_path')

        if ipfs_fn.in_storage():
            continue

        filename = cfs_fn.get_locally()
        ipfs.upload(filename, filename, timestamp = timestamp)


if __name__ == '__main__':
    tranferdata()
