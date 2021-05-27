from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf, SparkContext
import re
import sys

DAMPING_FACTOR = 0.8


# args: [1]=input_file(txt),   [2]=output_file(txt),    [3]=nIter

def data_parser(line):
    # get the index of the begin of the title
    begin_title_index = line.find("<title>") + len("<title>")
    # get the index of the end of the title
    end_title_index = line.find("</title>")
    # get the title
    page_title = line[begin_title_index:end_title_index]

    # get the index of the begin of the text section
    begin_text_index = line.find("<text") + len("<text")
    # get the index of the end of the text section
    end_text_index = line.find("</text>")
    # get the text section
    page_text_section = line[begin_text_index:end_text_index]
    # get all the outgoing_links (any character between 2 pairs of '[]'
    outgoing_links = re.findall(r'\[\[([^]]*)\]\]', page_text_section)

    return page_title, outgoing_links


def spread_rank(node, outgoing_links, rank):
    mass_to_send = 0
    if len(outgoing_links) > 0:
        mass_to_send = rank / len(outgoing_links)
    # the contribution of a node to itself is 0
    rank_list = [(node, 0)]
    for link in outgoing_links:
        rank_list.append((link, mass_to_send))
    return rank_list

spark = SparkSession.builder.appName('joins_example').getOrCreate()

# import context from Spark (distributed computing using yarn, name of the application)
sc = spark.sparkContext

# import input data from txt file to rdd
input_data_rdd = sc.textFile(sys.argv[1])

# count number of nodes in the input dataset
node_number = input_data_rdd.count()

# parse input rdd to get graph structure (k=title, v=[outgoing links])
nodes = input_data_rdd.map(lambda input_line: data_parser(input_line))

# set the initial pagerank (1/node_number), node[0] is the title of the page
page_ranks = nodes.map(lambda node: (node[0], 1 / node_number))

nodes_title = [node[0] for node in nodes]

for i in range(int(sys.argv[3])):
    full_nodes = nodes.join(page_ranks)
    print("\n\n\n\n\n\n\n\n\n\n\n\n")
    print(full_nodes.take(20))

    # computes masses to send (node_tuple[0] = title | node_tuple[1][0] = outgoing_links | node_tuple[1][1] = rank)
    contribution_list = full_nodes.flatMap(lambda node_tuple: spread_rank(node_tuple[0], node_tuple[1][0], node_tuple[1][1]))\
                                  .filter(lambda pair: pair[0] in nodes_title)
    print("\n\n\n\n\n\n\n\n\n\n\n\n")
    print(contribution_list.take(20))
    # inner join to consider only nodes inside the considered network
    # aggregate contributions for each node, compute final ranks
    page_ranks = contribution_list.reduceByKey(lambda x, y: x[1] + y[1]) \
        .mapValues(lambda summed_contributions:
                   (float(1 - DAMPING_FACTOR) / node_number) + (DAMPING_FACTOR * float(summed_contributions)))

    print("\n\n\n\n\n\n\n\n\n\n\n\n")
    print(page_ranks.take(20))

# swap key and value, sort by key (by pagerank) and swap again
page_ranks.map(lambda a, b: (b, a)) \
    .sortByKey(1, 1) \
    .map(lambda a, b: (b, a))

# save the output
page_ranks.saveAsTextFile(sys.argv[2])
