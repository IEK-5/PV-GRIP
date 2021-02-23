from open_elevation.celery_tasks \
    import CELERY_APP


@CELERY_APP.task()
def task_check_files_lrucache(**files_lrucache_args):
    from open_elevation.files_lrucache import Files_LRUCache
    cache = Files_LRUCache(**files_lrucache_args)
    cache.check_content()
