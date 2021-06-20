import os

from pvgrip.globals \
    import GRASS

from pvgrip.utils.run_command \
    import run_command


def call_rsun_irradiance(wdir, solar_time,
                         elevation = None,
                         aspect = None, aspect_value = None,
                         slope = None, slope_value = None,
                         linke = None, linke_value = None,
                         albedo = None, albedo_value = None,
                         coeff_bh = None, coeff_dh = None,
                         njobs = 4, npartitions = 4):
    grass_path = os.path.join(wdir, 'grass','PERMANENT')
    radout = ['beam_rad','diff_rad','refl_rad','glob_rad']

    run_command\
        (what = \
         [GRASS, grass_path,
          '--exec','r.sun'] + \
         ['%s=%s' % (x,x) for x in radout] + \
         ['day=%d' % int(solar_time['day']),
          'time=%f' % float(solar_time['hour']),
          'nprocs=%d' % int(njobs),
          'npartitions=%d' % int(npartitions)] + \
         ['elevation=%s' % elevation] if elevation else [] + \
         ['aspect=%s' % aspect] if aspect else [] + \
         ['aspect_value=%f' % float(aspect_value)] \
         if aspect_value else [] + \
         ['slope=%s' % slope] if slope else [] + \
         ['slope_value=%f' % float(slope_value)] \
         if slope_value else [] + \
         ['linke=%s' % linke] if linke else [] + \
         ['linke_value=%f' % float(linke_value)] \
         if linke_value else [] + \
         ['albedo=%s' % albedo] if albedo else [] + \
         ['albedo_value=%f' % float(albedo_value)] \
         if albedo_value else [] + \
         ['coeff_bh=%s' % coeff_bh] if coeff_bh else [] + \
         ['coeff_dh=%s' % coeff_dh] if coeff_dh else [],
         cwd = wdir)

    return radout


def call_rsun_incidence(wdir, solar_time,
                        njobs = 4, npartitions = 4):
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.sun',
                 'elevation=elevation',
                 'incidout=incidence',
                 'day=%d' % int(solar_time['day']),
                 'time=%f' % float(solar_time['hour']),
                 'nprocs=%d' % int(njobs),
                 'npartitions=%d' % int(npartitions)],
         cwd = wdir)

    return 'incidence'
