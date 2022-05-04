import re
import pandas as pd

from datetime \
    import datetime
from math \
    import radians

from pvgrip.ssdp.utils \
    import timestr2utc_time
from pvgrip.utils.run_command \
    import run_command


def solar_time(timestr, lat, lon):
    """Compute solar time with ssdp solpos

    """
    dt = datetime.strptime(timestr, '%Y-%m-%d_%H:%M:%S')
    unixtime = timestr2utc_time(timestr)

    lstout = run_command\
        (['lst','%d' % unixtime, '%.12f' % lon, '%.12f' % lat],
         cwd='.', return_stdout = True)
    dt = datetime.strptime(re.findall(r'LST: (.*)', lstout)[0],
                           '%Y-%m-%d %H:%M:%S')

    return {'day': dt.timetuple().tm_yday,
            'hour': dt.hour + dt.minute/60 + dt.second/3600}
