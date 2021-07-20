import json
import celery
import traceback

from pvgrip.utils.cache_fn_results \
    import cache_fn_results

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.storage.cassandra_path \
    import Cassandra_Path, is_cassandra_path


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


def get_job_results(job, timeout = 30):
    try:
        if 'SUCCESS' == job.state:
            fn = job.result

        if 'FAILURE' == job.state:
            raise job.result

        if 'PENDING' == job.state:
            fn = job.wait(timeout = timeout)

        return fn
    except TASK_RUNNING:
        return {'results': {'message': 'task is running'}}
    except celery.exceptions.TimeoutError:
        return {'results': {'message': 'task is running'}}
    except Exception as e:
        return return_exception(e)


def format_help(data):
    res = []
    for key, item in data.items():
        res += [("""%15s=%s
        %s
        """ % ((key,) + item)).lstrip()]

    return '\n'.join(res)


def call_method(task, args):
    return serve(call_task(task=task, args=args))


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def call_task(task, args):
    try:
        job = task(**args).delay()
    except Exception as e:
        return return_exception(e)

    return get_job_results(job)
