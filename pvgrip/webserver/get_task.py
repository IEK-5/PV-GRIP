import bottle

from pvgrip.raster.calls \
    import sample_raster
from pvgrip.osm.calls \
    import osm_render
from pvgrip.integrate.calls \
    import ssdp_integrate
from pvgrip.irradiance.calls \
    import irradiance_ssdp, irradiance_grass
from pvgrip.route.calls \
    import ssdp_route
from pvgrip.shadow.calls \
    import shadow, average_shadow
from pvgrip.weather.calls \
    import irradiance_route, irradiance_bbox, \
    reanalysis_route, reanalysis_bbox

from pvgrip.status.utils \
    import status

from pvgrip.globals \
    import get_SPATIAL_DATA, PVGRIP_CONFIGS, get_Tasks_Queues

from pvgrip.storage.upload \
    import upload


def get_task(method, args):
    """Determine what to do

    :method: a keyword

    :args: additional arguments passed

    :return: either a callable function that generates a queue of
    tasks, or non-callable that is send to user directly
    """
    from pvgrip.webserver.utils \
        import serve

    if 'raster' == method:
        run = sample_raster
    elif 'shadow' == method:
        run = shadow
    elif 'shadow/average' == method:
        run = average_shadow
    elif 'osm' == method:
        run = osm_render
    elif 'irradiance' == method:
        run = irradiance_ssdp
    elif 'irradiance/grass' == method:
        run = irradiance_grass
    elif 'irradiance/ssdp' == method:
        run = irradiance_ssdp
    elif 'route' == method:
        if 'tsvfn_uploaded' == 'NA':
            raise RuntimeError\
                ('tsvfn_uploaded must be provided!')

        run = ssdp_route
    elif 'integrate' == method:
        if 'tsvfn_uploaded' == 'NA':
            raise RuntimeError\
                ('tsvfn_uploaded must be provided!')

        run = ssdp_integrate
    elif 'status' == method:
        return {'results': status()}
    elif 'datasets' == method:
        SPATIAL_DATA = get_SPATIAL_DATA()
        return {'results': SPATIAL_DATA.get_datasets()}
    elif 'upload' == method:
        return serve(upload(bottle.request.files.data))
    elif 'download' == method:
        return serve(args['path'], args['serve_type'])
    elif 'weather/irradiance' == method:
        run = irradiance_bbox
    elif 'weather/irradiance/box' == method:
        run = irradiance_bbox
    elif 'weather/irradiance/route' == method:
        run = irradiance_route
    elif 'weather/reanalysis' == method:
        run = reanalysis_bbox
    elif 'weather/reanalysis/box' == method:
        run = reanalysis_bbox
    elif 'weather/reanalysis/route' == method:
        run = reanalysis_route
    else:
        raise RuntimeError\
            ('method={} is not implemented'\
             .format(method))

    return run
