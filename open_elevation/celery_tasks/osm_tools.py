import pyrosm
import logging
import os
import sys
import shutil
import esy.osm.pbf
import time
import subprocess
import heapq
from open_elevation.osm_get_roi import *
from open_elevation.files_lrucache import *

import json
import requests
import xml.etree.ElementTree as etree
import celery
import open_elevation.utils as utils
import open_elevation.celery_tasks.app as app
import geohash
import itertools



OVERPASS_URL = "http://overpass-api.de/api/interpreter?data="
LRUCache = Files_LRUCache(1)

@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def find_osm_data_online(bounding_box, tags=None):
    box = (bounding_box[0][0],bounding_box[1][0],bounding_box[0][1],bounding_box[1][1])

    query_tags = ""
    if tags:
        for tag in tags:

            query_tags = query_tags + f"""node{str(box)};
                              way[{str(tag)}]{str(box)};
                              relation[{str(tag)}]{str(box)};"""
                              
    else:
        query_tags = f"""node{str(box)};
                         way{str(box)};
                         relation{str(box)};"""
    query = f"""
    [out:xml];
    (
    {query_tags}
    
    );
    out center;
    """
    
    #We have to set referer. If this is not done we could be denied access to osm data. See: https://help.openstreetmap.org/questions/55828/overpass-returns-429-every-time-for-some-queries-but-not-for-others
    response = requests.get(OVERPASS_URL, params={'data':query}, headers={'referer':'open-elevation'})
    
    tmp_f = utils.get_tempfile()
    with open(tmp_f,'w') as file:
        file.write(response.text)
        
    return tmp_f
        
    




def find_common_tags(number_of_tags: int):
    """Finds a list of size number_of_tags of the most frequent tags. 
    """
    download_path = os.getcwd()+"/CityData"
    os.mkdir(download_path)
    pbf_file = pyrosm.get_data("Aachen", directory=download_path)
    
    osm = esy.osm.pbf.File(pbf_file)
    counter = dict()
    
    for i in osm:
                
        for tag in i[1].keys():
            if tag in counter:
                counter[tag] = counter[tag] + 1
            else:
                counter[tag] = 1
             
    return heapq.nlargest(number_of_tags, counter, key=counter.get)


def add_to_database():
    try:
        subprocess.check_call(['osm2pgsql','--slim','-a', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])
        
    except subprocess.CalledProcessError:
        subprocess.Popen(['osm2pgsql','--slim','-c', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])



def create_map_for_csv(data_path ,data: str):
    
    bounding_boxes = get_roi_csv(data)
    os.makedirs(data_path, exist_ok = True)
    for box in bounding_boxes:
        path_to_file, file_name = find_osm_data_online(box, ['building']) #It seems that only using speciffic tags prevents smrender from working correctly
        
        LRUCache.add(path_to_file)
        
        render_osm_data(box, path_to_file, file_name)
        
    
           

@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def create_rules(tag):
    #Normaly tag could contain a list of tags.
    
    root = etree.Element('osm')

    tree = etree.ElementTree(root)

    type_ = etree.Element('way')

    root.append(type_)

    tag_name = etree.Element('tag')
    type_.append(tag_name)

    tag_name.set('k',tag[0])
    tag_name.set('v','')

    tag_action = etree.Element('tag')
    type_.append(tag_action)

    tag_action.set('k','_action_')
    tag_action.set('v','draw:color=black;bcolor=black')

    tmp_f = utils.get_tempfile()
    tree.write(open(tmp_f,'wb'))
    
    return tmp_f


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def render_osm_data(data):
  
    #The data attribute contains the following file paths:
    #File path that describes the boundingbox in smrender syntax
    #File path that contains all information about the region inside the bounding box (osm information)
    #File path that contains the rules that are to be used with smrender
    
    bbox = data[0]

    tmp_f = utils.get_tempfile()
    
    try:
        renderer = subprocess.Popen(['smrender', '-i', data[1], '-o', "/code/temp.png", bbox, '-r', data[2]])
        renderer.wait()
    except:
        print(f"An error has occured!")
        
    os.rename("/code/temp.png",tmp_f)
    try:
        os.remove('/code/temp.png')
    except:
        pass
    
    return tmp_f
    
   
 
@app.CELERY_APP.task()
@app.one_instance(expire = 10)
def return_box(box):
    smrender_syntax = f"{str(box[0])}:{str(box[1])}:{str(box[2])}:{str(box[3])}"
    return smrender_syntax

def bbox2hash(bbox, hash_length):
    """Split bounding box coordinates onto smaller boxes

    :bbox: (lat_min, lon_min, lat_max, lon_max)

    :hash_length: maximum of the geohash string

    :return: list of hashes meshing the bbox
    """
    by = geohash.decode_exactly\
        (geohash.encode\
         (*bbox[:2])[:hash_length])[2:]
    res = (geohash.encode(*x)[:hash_length] \
           for x in itertools.product\
           (np.arange(bbox[0],bbox[2] + by[0],by[0]),
            np.arange(bbox[1],bbox[3] + by[1],by[1])))
    return list(set(res))


def get_bbox_list(box, hash_length):
    tmp_f = utils.get_tempfile()
    with open(tmp_f, 'w') as file:
        file.write(str([str(box), str(hash_length)]))
    hashes = bbox2hash(box, hash_length)
    f = [geohash.bbox(i) for i in hashes]
    y = [((x['s'],x['n']),(x['w'],x['e'])) for x in f]
    return y


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def merge_osm(osm_files):
    tmp_f = utils.get_tempfile()
    try:
        merger = subprocess.Popen(['osmconvert', *osm_files, '-o=/code/temp.osm'])
        merger.wait()
    except:
        print(f"An error has occured!")
        
    os.rename('/code/temp.osm',tmp_f)
    try:
        os.remove('/code/temp.osm')
    except:
        pass
    return tmp_f

def osm_render(box, tag = ['building']):
    
    
    bbox = get_bbox_list(box = box, hash_length = 5)
    
    tasks = celery.group(find_osm_data_online.signature(kwargs={'tags':tag, 'bounding_box':item}) for item in bbox)

    tasks |= celery.group(return_box.si(box), merge_osm.signature(), create_rules.si(tag))
    
    tasks |= render_osm_data.signature()
    
    return tasks

#tasks = osm_render((50.77343,6.08492, 50.77670,6.09045),['building'])

