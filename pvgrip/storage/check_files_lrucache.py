from pvgrip.celery_tasks \
    import CELERY_APP


@CELERY_APP.task()
def task_check_files_lrucache(**files_lrucache_args):
    from pvgrip.storage.files_lrucache import Files_LRUCache
    cache = Files_LRUCache(**files_lrucache_args)
    cache.check_content()
