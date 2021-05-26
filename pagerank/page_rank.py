from pyspark import SparkConf, SparkContext
import re
import sys


# args: [1]=input_file(txt),   [2]=output_file(txt),    [3]=nIter

def data_parser(line):
    begin_title_index = line.find("<title>") + len("<title>")
    end_title_index = line.find("</title>")
    page_title = line[begin_title_index:end_title_index]

    # get all the outgoing_links (any character between 2 pairs of '[]'
    outgoing_links = re.findall(r'\[\[([^]]*)\]\]', line)
    return page_title, outgoing_links

def spread_rank(outgoing_links, rank):
    neighbors_number = len(outgoing_links)
    rank_list = []
    for link in outgoing_links:



# import context from Spark (distributed computing using yarn, name of the application)
sc = SparkContext("yarn", "page_rank_baggins")

# import input data from txt file to rdd
input_data_rdd = sc.textFile(sys.argv[1])

# count number of nodes in the input dataset
node_number = input_data_rdd.count()

# parse input rdd to get graph structure (k=title, v=[outgoing links])
nodes = input_data_rdd.map(lambda input_line: data_parser(input_line))

# set the initial pagerank (1/node_number), node[0] is the title of the page
page_ranks = nodes.map(lambda node: (node[0], 1/node_number))


for i in range (int(sys.argv[3])):
    full_nodes = nodes.join(page_ranks)
    # computes masses to send (node_tuple[0] = [outgoing_links], node_tuple[1]=rank)
    contribution_list = full_nodes.flatMap(lambda title, node_tuple: spread_rank(node_tuple[0], node_tuple[1]))
# page rank iter
# sorting
