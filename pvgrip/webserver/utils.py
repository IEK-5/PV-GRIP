import re
import json
import celery
import traceback

from celery.result \
    import AsyncResult

from pvgrip \
    import CELERY_APP
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.cache_fn_results \
    import cache_fn_results

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.storage.cassandra_path \
    import Cassandra_Path, is_cassandra_path

from pvgrip.globals \
    import get_Tasks_Queues, PVGRIP_CONFIGS

from pvgrip.webserver.tasks \
    import generate_task_queue


def return_exception(e):
    return {'results':
            {'error': type(e).__name__ + ": " + str(e),
             'traceback': traceback.format_exc()}}


def parse_args(data, defaults):
    res = {}

    if not data:
        return res

    for key, _ in data.items():
        if key not in defaults:
            raise RuntimeError("Unknown argument: '%s'" % key)

    if not defaults:
        return res

    for key, item in defaults.items():
        item = item[0]
        if key not in data:
            res[key] = item
        else:
            new = data[key]
            if type(item) != type(new) \
               and isinstance(new, str) \
               and isinstance(item, (list, dict)):
                new = json.loads(new)
            res[key] = type(item)(new)

    return res


def serve(data):
    if isinstance(data, dict):
        return data

    if is_cassandra_path(data):
        with open(Cassandra_Path(data).get_locally(),
                  'rb') as f:
            return f.read()

    return data


def _cleanup_job(job, key = None):
    job.forget()
    if key:
        tasks_queues = get_Tasks_Queues()
        del tasks_queues[key]


def get_job_results(job_id, key, timeout):
    job = AsyncResult(job_id)

    try:
        if 'SUCCESS' == job.state:
            fn = job.result

        if 'PENDING' == job.state:
            fn = job.wait(timeout = timeout)

        if 'FAILURE' == job.state:
            raise job.result
    except TASK_RUNNING:
        return {'results': {'message': 'task is running'}}
    except celery.exceptions.TimeoutError:
        return {'results': {'message': 'task is running'}}
    except Exception as e:
        _cleanup_job(job, key)
        return return_exception(e)

    # this line is not reached in case TASK_RUNNING
    _cleanup_job(job, key)

    return fn


def format_help(data):
    res = []
    for key, item in data.items():
        res += [("""%15s=%s
        %s
        """ % ((key,) + item)).lstrip()]

    return '\n'.join(res)


def _method_results(method, args):
    tasks_queues = get_Tasks_Queues()
    job_id = tasks_queues[(method, args)]

    # determine if task is generate_task_queue type
    m = re.match('generate_task_queue://(.*)', job_id)
    if m:
        job_id = get_job_results\
            (m.groups()[0],
             timeout = \
             int(PVGRIP_CONFIGS['webserver']['queue_timeout']),
             key = (method, args))

    if isinstance(job_id, dict):
        return job_id

    tasks_queues[(method, args)] = job_id

    return get_job_results\
        (job_id,
         timeout = \
         int(PVGRIP_CONFIGS['webserver']['task_timeout']),
         key = (method, args))


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def call_method(method, args):
    tasks_queues = get_Tasks_Queues()

    if (method, args) in tasks_queues:
        return _method_results(method, args)

    try:
        job = generate_task_queue.delay(method, args)
        tasks_queues[(method, args)] = \
            "generate_task_queue://{}".format(job.task_id)
    except Exception as e:
        return return_exception(e)

    return _method_results(method, args)
