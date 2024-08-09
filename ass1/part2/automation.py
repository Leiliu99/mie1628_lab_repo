#!/usr/bin/env python
"""automation.py"""
#This is the script to run automation flow for 15 iterations of Map Reduce
#input the cluster number k when you run this script

import sys
import subprocess
import time

def check_convergence(prev, current):
    return prev == current

if __name__ == "__main__":
    #record time
    start_time = time.time()

    #get cluster number k from the input
    k = sys.argv[1]
    max_iteration = 15
    print("-----Input k value as " + k + "-----")

    #Initialize the centroids.txt given k value
    #choose the first k lines be the centroids at the very begining
    print("-----Initialize centroids.txt-----")
    prev_result = ""
    init_i = 0
    centroids = []
    with open("/Users/liulei/Desktop/mie1628/ass1/part2/data_points.txt", 'r') as input_data:
        for line in input_data:
            if init_i < int(k):
                x,y = line.split(',')
                centroids.append(f"{init_i},{x},{y}")
                prev_result+=(f"{init_i},{x},{y}")
                init_i += 1
            else:
                break
    #write into centroids.txt
    with open("/Users/liulei/Desktop/mie1628/ass1/part2/centroids.txt", 'w') as input_centroids:
        for line in centroids:
            input_centroids.write(line)

    #run iterations for hdfs command
    iter = 0
    converge = False

    while not converge and iter < max_iteration:
        print(f"-----Running iteration {iter}-----")
        if iter > 0:
            print("-----Remove previous iteration's result-----")
            remove_command = ["bin/hdfs", "dfs", "-rm", "-r", "/ass1-2_out"]
            subprocess.run(remove_command)

        streaming_command = ["bin/hadoop", "jar", "/Users/liulei/mie1628_hadoop/hadoop-3.4.0/share/hadoop/tools/lib//hadoop-streaming-3.4.0.jar",
        "-file", "/Users/liulei/Desktop/mie1628/ass1/part2/mapper.py", "-mapper", "\"python3 mapper.py\"",
        "-file", "/Users/liulei/Desktop/mie1628/ass1/part2/reducer.py", "-reducer", "\"python3 reducer.py\"",
        "-input", "/ass1-2/*",
        "-output", "/ass1-2_out"]
        streaming_result = subprocess.run(streaming_command)
        if not streaming_result.returncode == 0:
            print("*****Streaming process not success!*****")
            exit(0)


        #using the steaming result to update centroids.txt
        print(f"-----Updating new centroids in iteration {iter}-----")

        cat_command = ["bin/hadoop", "dfs", "-cat", "/ass1-2_out/part-00000"]
        cat_result = subprocess.check_output(cat_command).decode().strip()
        print(".....Dumping previous result!.....")
        print(prev_result)
        print(".....Dumping current result!.....")
        with open("/Users/liulei/Desktop/mie1628/ass1/part2/centroids.txt", 'w') as input_centroids:
            input_centroids.write(cat_result)
        print(cat_result)

        converge = check_convergence(prev_result, cat_result)
        #update prev result for next convergence check
        cat_result = prev_result

        iter += 1

    end_time = time.time()
    duration = end_time - start_time

    print(f"-----Successfully run all iterations, total duration: {duration}-----")

