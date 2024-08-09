#!/usr/bin/env python
"""reducer.py"""

import sys

def calculateNewCentroids():
    '''
    Reducer function:
    Similar to combiner except it is for all machines
    load in what combiner gives
    Combines member, member centroid for each cluster from different machines
    together.
    Update Centroid for each cluster
    output exactly the same format of data as input file except centroid, cluster
    ID have been updated
    '''
    current_centroid = None
    accumulate_x = 0
    accumulate_y = 0
    current_count = 0


    # parse the input of mapper.py
    # input comes from STDIN
    for line in sys.stdin:
        id, coordinates = line.split(" ")
        # convert x and y (currently a string) to float
        x, y = map(float, coordinates.split(","))

        if id != current_centroid:
            if current_centroid is not None:
                # print the average of every cluster to get new centroids
                avg_x = accumulate_x / current_count
                avg_y = accumulate_y / current_count
                print(f"{current_centroid},{avg_x},{avg_y}")
            # re-initialization for new cluster
            current_centroid = id
            accumulate_x = 0
            accumulate_y = 0
            current_count = 0

        #accumulate
        accumulate_x += x
        accumulate_y += y
        current_count += 1
    
    # print last cluster's centroids
    if current_centroid is not None:
        # print the average of every cluster to get new centroids
        avg_x = accumulate_x / current_count
        avg_y = accumulate_y / current_count
        print(f"{current_centroid},{avg_x},{avg_y}")
    

if __name__ == "__main__":
    calculateNewCentroids()