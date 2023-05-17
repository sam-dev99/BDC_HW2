import time
from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
# import timeit
import argparse
from collections import defaultdict
import itertools

import collections

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = [int(c) for c in colors_tuple] 
    #Create a dictionary for adjacency list
    neighbors = collections.defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:
        u, v = edge
        u = int(u)
        v = int(v)
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:        
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:            
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count


def hc(u, C,a,b):
    # Hash function to map each vertex
    p = 8191
    return ((a*u + b) % p) % C


def check_if_two_nodes_are_equal(edge, C,a,b):
    
    hc_u = hc(int(edge[0]), C,a,b)
    hc_v = hc(int(edge[1]), C,a,b)

    if (hc_u == hc_v):
        # print("i: " + str(hc_u))
        return [(hc_u, (edge[0], edge[1]))]

    return [(99999, (0, 0))]

# C color
# R repetitions

def triangle_count_per_color(subgraph):
    pairs_dict = {}
    pairs_dict[subgraph] = CountTriangles(subgraph[1])
    
    return [(key, pairs_dict[key]) for key in pairs_dict.keys()]

def triangle_count_per_color_2(subgraph,a,b,p,C):
    pairs_dict = {}
    pairs_dict[subgraph] = countTriangles2(subgraph[0], subgraph[1],a,b,p,C)
    
    return [(key, pairs_dict[key]) for key in pairs_dict.keys()]

def MR_ApproxTCwithNodeColors(edges, C):
    p = 8191
    a = rand.randint(1, p-1)
    b = rand.randint(0, p-1)
    
    triangles_count = edges.flatMap(lambda e : [(e.split(','))])  # <-- MAP PHASE (R1)
    triangles_count = triangles_count.flatMap(lambda x: check_if_two_nodes_are_equal(x, C,a,b))  # <-- MAP PHASE (R1)
    triangles_count = triangles_count.groupByKey()
    triangles_count = triangles_count.flatMap(triangle_count_per_color)
    #triangles_count = triangles_count.mapValues(lambda x: sum(x)) # <-- REDUCE PHASE (R2)
    triangles_count = triangles_count.values().sum() 
    return triangles_count*C*C

def permutations_with_repetition(items, length=None):
    # Generate all permutations of the items with the given length
    permutations = itertools.product(items, repeat=length)
    
    # Convert each permutation to a tuple
    permutations = [tuple(p) for p in permutations]
    
    return permutations

def unique_tuples(tuples):
    # Create an empty set to hold the unique tuples
    unique_tuples = set()
    
    # Iterate over each tuple in the original list
    for t in tuples:
        # Sort the tuple so that the elements are in a consistent order
        sorted_t = tuple(sorted(t))
        
        # If the sorted tuple is not already in the set, add it
        if sorted_t not in unique_tuples:
            unique_tuples.add(sorted_t)
    
    # Convert the set of unique tuples back to a list
    unique_tuples_list = list(unique_tuples)
    
    return unique_tuples_list

def tuple_in_list(tuples, target_tuple):
    # Sort the target tuple so that the elements are in a consistent order
    sorted_target_tuple = tuple(sorted(target_tuple))
    
    # Iterate over each tuple in the original list
    for t in tuples:
        # Sort the tuple so that the elements are in a consistent order
        sorted_t = tuple(sorted(t))
        
        # If the sorted tuple matches the sorted target tuple, return True
        if sorted_t == sorted_target_tuple:
            return True
    
    # If the target tuple is not found in the list, return False
    return False


def sort_tuples_lexicographically(tuples):
    # Sort the list of tuples lexicographically
    sorted_tuples = sorted(tuples)
    
    return sorted_tuples


def return_key_with_edges(edge,C,a,b,sorted_tuples):
    # print("entered the function of return key")
    
    hc_u = hc(int(edge[0]), C,a,b)
    hc_v = hc(int(edge[1]), C,a,b) 

    lst_of_edges_with_same_key = []

    for i in range(C):
        tuple_to_check = (i,hc_u,hc_v)
        if(tuple_in_list(sorted_tuples,tuple_to_check)):
            key = ''.join(map(str,sorted(tuple_to_check))) #001,002...

            lst_of_edges_with_same_key.append((key, (edge[0], edge[1])))

    return lst_of_edges_with_same_key


def MR_ExactTC(edges, C,sorted_tuples):
    p = 8191
    a = rand.randint(1, p-1)
    b = rand.randint(0, p-1)

    triangles_count = edges.flatMap(lambda e : [(e.split(','))])  # <-- MAP PHASE (R1)
    triangles_count = triangles_count.flatMap(lambda x: return_key_with_edges(x, C,a,b,sorted_tuples))  # <-- MAP PHASE (R1)

    # elems = triangles_count.collect()
    # for elem in elems:
    #     print(elem)

    # triangles_count = triangles_count.reduceByKey(lambda graph : triangle_count_per_color_2(graph,a,b,p,C))
    # triangles_count = triangles_count.reduceByKey(lambda x,y: x*2)
    # print("\n\nbefore groupbykey")
    triangles_count = triangles_count.groupByKey()
    # print("\nafter groupbykey\n")
    triangles_count = triangles_count.flatMap(lambda graph : triangle_count_per_color_2(graph,a,b,p,C))
    triangles_count = triangles_count.values().sum() #R2 
    return triangles_count
    # elems = triangles_count.collect()
    # for elem in elems:
    #     print(elem)
    




def main():

    # check if the number of arguments is correct
    if len(sys.argv) != 5:
        print("Usage: python G064HW1.py <C> <R> <F> <graph_file>")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='Parsing passed arguements')
    parser.add_argument('C', type=int, help='Number of partitions')
    parser.add_argument('R', type=int, help='Number of repetitions')
    parser.add_argument('F', type=int, help='binary Flag [0 - 1]')
    parser.add_argument('rawData', type=str, help='Path to graph file')

    # Access the parsed arguments using args.C, args.R, and args.rawData
    args = parser.parse_args()

    # 1
    C = args.C

    # 2
    R = args.R

    # 3
    F = args.F

    # SPARK SETUP
    conf = SparkConf().setAppName('G064HW2')
    sc = SparkContext(conf=conf)

    # 4. Read input file and subdivide it into K random partitions
    rawData = args.rawData
    assert os.path.isfile(rawData), "File not found"
    edges = sc.textFile(rawData, minPartitions=C).cache()
    edges = edges.repartition(numPartitions=C)


    # SETTING GLOBAL VARIABLES
    numedges = edges.count()

    # 5. print information about the graph
    print("Dataset = " + rawData)
    print("Number of edges = ", numedges)
    print("Number of Colors = ", C)
    print("Number of Repetitions = ", R)
    print("Approximation through node coloring")
    start = time.time()


    if(F == 1):
        # print(MR_ExactTC(edges,C))
        items = [i for i in range(C)]
        permutations = permutations_with_repetition(items, length=3)


        unique_tuples_list = unique_tuples(permutations) 
        sorted_tuples = sort_tuples_lexicographically(unique_tuples_list)  #kis (hc(u),hc(v),i) order doesnt matter

        exact_number_of_triangles_from_R_Runs = 0 
        for i in range(R):
            exact_number_of_triangles_from_R_Runs = MR_ExactTC(edges,C,sorted_tuples) 

        print("- Number of triangles = {}".format(str(exact_number_of_triangles_from_R_Runs)))
        print("- Running time (average over {} runs) = {} {}".format(R, int((time.time() - start)*1000)//R," ms"))
    
        

    else:   
        total_number_of_triangles_from_R_Runs = []
        for i in range(R):
            total_number_of_triangles_from_R_Runs.append(MR_ApproxTCwithNodeColors(edges,C)) 

        # lst = list(total_number_of_triangles_from_R_Runs)
        total_number_of_triangles_from_R_Runs.sort()
        print("- Number of triangles (median over {} runs) = {}".format(R,str(total_number_of_triangles_from_R_Runs[len(total_number_of_triangles_from_R_Runs)//2])))
        print("- Running time (average over {} runs) = {} {}".format(R, int((time.time() - start)*1000)//R," ms"))
    
    sc.stop()

if __name__ == "__main__":
    main()
