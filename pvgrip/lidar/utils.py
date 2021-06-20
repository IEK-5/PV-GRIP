import json


def write_pdaljson(laz_fn, ofn, resolution, what, json_ofn):
    data = {}
    data['pipeline'] = [{
        'type': 'readers.las',
        'filename': laz_fn}]
    # ignore noise points
    data['pipeline'] += [{
        'type': 'filters.range',
        'limits': 'Classification![18:18]'}]
    data['pipeline'] += \
    [{'filename': ofn,
      'gdaldriver': 'GTiff',
      'output_type': what,
      'resolution': resolution,
      'type': 'writers.gdal'}]

    with open(json_ofn, 'w') as f:
        json.dump(data, f)
