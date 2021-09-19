import re

from datetime import datetime, timedelta

timestr_re = \
    r'[0-9]{4}-[0-9]{2}-[0-9]{2}_' + \
    r'[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'
range_re = '({x})/({x})'.format(x = timestr_re)
timestr_re = re.compile('^' + timestr_re + '$')
range_re = re.compile('^' + range_re + '$')

time_step_re = re.compile(r'([0-9]+)([a-z]+)')


def timestr2datetime(timestr):
    return datetime.strptime(timestr,'%Y-%m-%d_%H:%M:%S')


def time_step2seconds(time_step):
    """Convert time_step to seconds

    :time_step: string in a format '<integer><units>',
    where unit is second, minute, hour or day

    :return: step converted to seconds
    """
    if not time_step_re.match(time_step):
        raise RuntimeError\
            ("invalid time_step = {}"\
             .format(time_step))

    value, unit = time_step_re.findall(time_step)[0]

    if re.match(r'second[s]?', unit):
        unit_delta = 1
    elif re.match(r'minute[s]?', unit):
        unit_delta = 60
    elif re.match(r'hour[s]?', unit):
        unit_delta = 60*60
    elif re.match(r'day[s]?', unit):
        unit_delta = 60*60*24
    else:
        raise RuntimeError\
            ("invalud unit = {}"\
             .format(unit))

    return int(value)*unit_delta


def seq_days(date_from, date_to, time_step):
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    numseconds = (date_to - date_from).total_seconds()
    delta = time_step2seconds(time_step)
    N = int(numseconds / delta) + 1
    return [date_from + timedelta(seconds = delta*x)\
            for x in range(N)]


def time_range2list(time_range, time_step = '1day',
                    time_format = '%Y-%m-%d'):
    """Process time range and generate list of days

    :time_range: string, either timestr or timestr/timestr

    :time_step: step between times in the list

    :time_format: format of dates in the list

    :return: list of times
    """
    if timestr_re.match(time_range):
        return [datetime.strftime\
                (timestr2datetime(time_range),time_format)]

    if not range_re.match(time_range):
        raise RuntimeError("Invalid time_range = {}"\
                           .format(time_range))

    date_from, date_to = range_re.findall(time_range)[0]
    return [datetime.strftime(x,time_format) \
            for x in seq_days(timestr2datetime(date_from),
                              timestr2datetime(date_to),
                              time_step = time_step)]
