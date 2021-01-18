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
from xml.etree import ElementTree

OVERPASS_URL = "http://overpass-api.de/api/interpreter"
LRUCache = Files_LRUCache(1)


def find_osm_data_online(data_path, bounding_box, tags):
    box = (bounding_box[0][0],bounding_box[1][0],bounding_box[0][1],bounding_box[1][1])

    query_tags = ""
    for tag in tags:
        
        query_tags = query_tags + f"""node[{str(tag)}]{str(box)};
                          way[{str(tag)}]{str(box)};
                          relation[{str(tag)}]{str(box)};"""
        
    
    query = f"""
    [out:xml];
    (
    {query_tags}
    
    );
    out center;
    """

    response = requests.get(OVERPASS_URL, params={'data':query})

    file_name = f'{box}.osm'
    path_to_file = f'{data_path}/{file_name}'
    with open(path_to_file,'w') as file:
        file.write(response.text)
        
    return path_to_file
        
    




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
    

def add_to_database():
    try:
        subprocess.check_call(['osm2pgsql','--slim','-a', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])
        
    except subprocess.CalledProcessError:
        subprocess.Popen(['osm2pgsql','--slim','-c', '-d', 'osm_database', '-H', 'localhost', '-P','8080','-U','osm_user', 'data.osm'])


def create_map_for_data(data_path ,data: str):
    
    
    #most_common_tags = find_common_tags(10)
    bounding_boxes = get_roi(data)
    os.makedirs(data_path, exist_ok = True)
    for box in bounding_boxes:
        
        path_to_file = find_osm_data_online(data_path ,box, ['building', 'amenity', 'landuse','natural'])
        
        LRUCache.add(path_to_file)
           
        

def time_func(function, parameter:tuple):
    #Save the starting time to check the runtime
    start = time.time()
    function(parameter)
    print(time.time()-start)


#create_map_for_data('./_osm_data','coordinates.csv')

