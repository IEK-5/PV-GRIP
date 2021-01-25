import pyrosm
import os
import sys
import resource
import esy.osm.pbf
import time
import subprocess
import heapq
from osm_get_roi import *
from files_lrucache import *

import json
import requests
import xml.etree.ElementTree as etree

import open_elevation.utils as utils
import open_elevation.celery_tasks.app as app

OVERPASS_URL = "http://overpass-api.de/api/interpreter"
LRUCache = Files_LRUCache(1)

@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def find_osm_data_online(data_path, bounding_box, tags=None):
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

    file_name = f"{str(box).replace('(','').replace(')','')}.osm"
    path_to_file = f'{data_path}/{file_name}'
    with open(path_to_file,'w') as file:
        file.write(response.text)
        
    return path_to_file, file_name
        
    



@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def find_common_tags(number_of_tags: int):
    """Finds a list of size number_of_tags of the most frequent tags. 
    """
    download_path = os.getcwd()+"/CityData"
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
    
@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def add_to_database():
    try:
        subprocess.check_call(['osm2pgsql','--slim','-a', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])
        
    except subprocess.CalledProcessError:
        subprocess.Popen(['osm2pgsql','--slim','-c', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])



@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def create_map_for_csv(data_path ,data: str):
    
    bounding_boxes = get_roi_csv(data)
    os.makedirs(data_path, exist_ok = True)
    for box in bounding_boxes:

        path_to_file, file_name = find_osm_data_online(data_path ,box) #It seems that only using speciffic tags prevents smrender from working correctly
        
        LRUCache.add(path_to_file)
        
        render_osm_data(box, path_to_file, file_name)
           

#The function is not yet finished
def create_rules():
    
    root = etree.Element('osm')

    tree = etree.ElementTree(root)

    type_ = etree.Element('way')

    root.append(type_)

    tag_name = etree.Element('tag')
    type_.append(tag_name)

    tag_name.set('k','buildings')
    tag_name.set('v','')

    tag_action = etree.Element('tag')
    type_.append(tag_action)

    tag_action.set('k','_action_')
    tag_action.set('v','draw:color=black;bcolor=black')

    tree.write(open('rules.osm','wb'))        


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def render_osm_data(box, input_xml, output_name):
  
    
    boundingBoxs = str(box[0][0])+':'+str(box[1][0])+':'+ str(box[0][1])+':'+str(box[1][1])

    try:
        renderer = subprocess.Popen(['smrender', '-i', input_xml, '-o', f'{output_name}.pdf','-P','A4', '-l', boundingBoxs, '-r', 'rules.osm'])
        renderer.wait()
    except:
        print(f"An error has occured!")



#create_map_for_data('./_osm_data','coordinates.csv')

