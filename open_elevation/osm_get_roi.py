import pandas as pd
import numpy as np
from typing import Tuple, List, Set

#Custom types
Quadrant_Set = Set[Tuple[float, float]]
Quadrant = Tuple[int, int]





def clean_data(data):
    #Drop all the duplicates that come from statonary data
    data.drop_duplicates(inplace=True)
    data_points = data.values
    return data_points




def quadrant_to_boundaries(quadrant: int, min_: float, d: float) -> (float,float):
    """Each quadrant point is mapped to the corresponding boundaries. 
       The input is either the lat or the lon of the quadrant.

    Args:
        quadrant (int): lat or lon value of the quadrant
        min_ (float): min of either the lat or lon value of the mesh
        d (float): distance between either the lat values or lon values of the mesh

    Returns:
        [type]: [description]
    """
    return min_ + quadrant*d, min_ + (quadrant+1)*d
    
 
def point_to_quadrant(point: float,  min_: float, d: float):

    y = (point-min_)/d
    
    return int(y)



        
def binning(data_points, min_lat: float, distance_lat: float, min_lon:float, distance_lon:float, lambda_:float) -> Quadrant_Set:
    quadrants_to_take = set()
    print(data_points)
    for point in data_points:
        upper_left = (point[0]+lambda_, point[1]-lambda_)
        upper_right = (point[0]+lambda_, point[1]+lambda_)
        lower_left = (point[0]-lambda_, point[1]-lambda_)
        lower_right = (point[0]-lambda_, point[1]+lambda_)
        
        
        quadrant_up_l = (point_to_quadrant(upper_left[0], min_lat, distance_lat), 
                         point_to_quadrant(upper_left[1], min_lon, distance_lon))
        
        quadrant_up_r = (point_to_quadrant(upper_right[0], min_lat, distance_lat), 
                         point_to_quadrant(upper_right[1], min_lon, distance_lon))
        
        quadrant_low_l = (point_to_quadrant(lower_left[0], min_lat, distance_lat), 
                         point_to_quadrant(lower_left[1], min_lon, distance_lon))
        
        quadrant_low_r = (point_to_quadrant(lower_right[0], min_lat, distance_lat), 
                         point_to_quadrant(lower_right[1], min_lon, distance_lon))
        
        lat = point_to_quadrant(point[0], min_lat, distance_lat)
        lon = point_to_quadrant(point[1], min_lon, distance_lon)
        
        quadrants_to_take.add(quadrant_up_l)
        quadrants_to_take.add(quadrant_up_r)
        quadrants_to_take.add(quadrant_low_l)
        quadrants_to_take.add(quadrant_low_r)
        quadrants_to_take.add((lat,lon))

    
    return quadrants_to_take
    

def get_mesh(csv_file, chunksize):
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        data_points = chunk.values
        
        min_lat = min(np.vstack([data_points, [min_lat,0]]), key = lambda x: x[0])[0]
        min_lon = min(np.vstack([data_points, [0,min_lon]]), key = lambda x: x[1])[1]
        max_lat = max(np.vstack([data_points, [max_lat,0]]), key = lambda x: x[0])[0]
        max_lon = max(np.vstack([data_points, [0,max_lon]]), key = lambda x: x[1])[1]
        
    return min_lat, min_lon, max_lat, max_lon
        



def get_roi_csv(csv_file ,chunksize=10**6, lambda_=None, n_segments_lat=None, n_segments_lon=None, segments_lat_distance=0.05, segments_lon_distance=0.05, lambda_d = 4) -> [((float,float),(float,float))]:
    """Given a chunksize and .csv file containing geo-points, 
        this function creates a set of rectangles that contain
        the geo-points and a region around them defined by lambda_.\n

    Args:\n
        chunksize (int): The size of the chunk that is to be read from the .csv file
        lambda_ (float): 
    """
   
    #min_lat, min_lon, max_lat, max_lon = get_mesh(csv_file, chunksize)
    min_lon, min_lat, max_lon, max_lat = -180, -90, 180, 90
    quadrant_boundaries = set()
    #Can be used to specify chunk size if not all the data can be loaded at once
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        
        data_points = clean_data(chunk)
        
        
        if not n_segments_lat:
            n_segments_lat = (max_lat-min_lat)/segments_lat_distance
            
        if not n_segments_lon:
            n_segments_lon = (max_lon-min_lon)/segments_lon_distance
        
        #Devide the segments
        lat_segments = np.linspace(min_lat, max_lat, num= int(n_segments_lat))
        lon_segments = np.linspace(min_lon, max_lon, num= int(n_segments_lon))

        if len(lat_segments) > 1:
            distance_lat = abs(lat_segments[1]-lat_segments[0])
        
        else:
            distance_lat = abs(max_lat-min_lat)
        
        if len(lon_segments) > 1:
            distance_lon = abs(lon_segments[1]-lon_segments[0])
        
        else:
            distance_lon = abs(max_lon - min_lon)
        
        max_lambda_ = min(distance_lat, distance_lon)
        
        if not lambda_:
            lambda_ = max_lambda_/lambda_d
            
        try:
            if max_lambda_ < lambda_ or lambda_<0:
                raise ValueError(lambda_)
            
        except ValueError:
            print(f"The lambda_ argument is too big or negative.\nYou picked lambda_ = {lambda_} \nPick lambda_ smaller than:\n{max_lambda_} and positive.")
            break
        
        else:
           
            quadrants_to_take = binning(data_points, min_lat, distance_lat, min_lon, distance_lon, lambda_)
            for i,j in quadrants_to_take:
                quadrant_boundaries.add((quadrant_to_boundaries(i,min_lat,distance_lat),quadrant_to_boundaries(j,min_lon,distance_lon)))
            
    return list(quadrant_boundaries)

