#!/usr/bin/env python
"""mapper.py"""

import sys
from math import sqrt

# get initial centroids from a txt file and add them in an array
def getCentroids(filepath):
    '''
    Mapper function:
        load in coordinates, Centroid, 
        Calculate the distances from each sample to each centroid point
        output relabeled cluster ID as key, Coordinates as value.
    
    '''
    with open(filepath, 'r') as file:
        centroids = []
        for line in file:
            content = line.split(',')
            cluster_id = content[0]
            coordinates = (float(content[1]), float(content[2]))
            centroids.append((cluster_id, coordinates))

    # print(centroids)
    return centroids

# create clusters based on initial centroids
def createClusters(centroids):
    # get input from stdin
    for line in sys.stdin:
        #get data coordinates
        content = line.split(',')
        data_x = float(content[0])
        data_y = float(content[1])
        nearest_id = ''
        nearest_distance = float("inf")
        for center in centroids:
            id, coordinates = center
            distance = sqrt((data_x-coordinates[0])**2 + (data_y-coordinates[1])**2)
            if distance < nearest_distance:
                nearest_distance = distance
                nearest_id = id
        print(f"{nearest_id} {data_x},{data_y}")

if __name__ == "__main__":
    centroids = getCentroids('/Users/liulei/Desktop/mie1628/ass1/part2/centroids.txt')
    createClusters(centroids)