import pyrosm
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


OVERPASS_URL = "http://overpass-api.de/api/interpreter"
LRUCache = Files_LRUCache(1)

@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def find_osm_data_online(bounding_box, tags=None):
    box = (bounding_box[0][0],bounding_box[1][0],bounding_box[0][1],bounding_box[1][1])

    query_tags = ""
    if tags:
        for tag in tags:

            query_tags = query_tags + f"""node[{str(tag)}]{str(box)};
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

    response = requests.get(OVERPASS_URL, params={'data':query})
    
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
    
    
    root = etree.Element('osm')

    tree = etree.ElementTree(root)

    type_ = etree.Element('way')

    root.append(type_)

    tag_name = etree.Element('tag')
    type_.append(tag_name)

    tag_name.set('k',tag)
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
def render_osm_data(box, input_xml, rules_file):
  
    
    boundingBoxs = str(box[0][0])+':'+str(box[1][0])+':'+ str(box[0][1])+':'+str(box[1][1])

    tmp_f = utils.get_tempfile()
    tmp_d = utils.get_tempdir()
    try:
        #You can use run_command instead which is implemented in utils.py
        file_path = os.path.join(tmp_d, 'map.pdf')
        renderer = subprocess.Popen(['smrender', '-i', input_xml, '-o', file_path,'-P','A4', '-l', boundingBoxs, '-r', rules_file])
        renderer.wait()
    except:
        print(f"An error has occured!")
        
    finally:
        os.rename(file_path, tmp_f)
        shutil.rmtree(tmp_d)
    
    return tmp_f
        



def osm_render(box, tag = 'building'):
    
    tasks = celery.group(find_osm_data_online.signature(kwargs = {'bounding_box' : box, 'tags':tag}),
                         create_rules.signature(kwargs={'tag' : tag}))
    
    tasks |= render_osm_data.signature(kwargs = {'box' : box})
    
    return tasks

#tasks = osm_render(((50.83912197832251, 50.88913587107049), (6.125850812597889, 6.17585775800697)),'building')

