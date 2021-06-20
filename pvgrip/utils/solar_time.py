from datetime \
    import datetime, time, timedelta
from math \
    import pi, cos, sin


def solar_time(timestr, lon):
    """Compute solar time

    Taken from:
    https://stackoverflow.com/a/13424528
    www.esrl.noaa.gov/gmd/grad/solcalc/solareqns.PDF

    :timestr: date and time UTC string in the format
    YYYY-MM-DD_HH:MM:SS]

    :lon: longitude

    :return: solar time {'day': int, 'hour': float}
    """
    dt = datetime.strptime(timestr, '%Y-%m-%d_%H:%M:%S')
    gamma = 2*pi/365*\
        (dt.timetuple().tm_yday-1+float(dt.hour-12)/24)
    eqtime = 229.18*\
        (0.000075+0.001868*cos(gamma)-\
         0.032077*sin(gamma)-\
         0.014615*cos(2*gamma)-\
         0.040849*sin(2*gamma))
    decl = 0.006918-0.399912*cos(gamma)+\
        0.070257*sin(gamma)-0.006758*cos(2*gamma)+\
        0.000907*sin(2*gamma)-0.002697*cos(3*gamma)+\
        0.00148*sin(3*gamma)
    time_offset = eqtime+4*lon
    tst = dt.hour*60+dt.minute+dt.second/60+time_offset
    s = datetime.combine(dt.date(),time(0))+\
        timedelta(minutes=tst)
    return {'day': s.timetuple().tm_yday,
            'hour': s.hour + s.minute/60 + s.second/(60*60)}
