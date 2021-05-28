from pyspark import SparkContext, SparkConf
import re
import sys

DAMPING_FACTOR = 0.8


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
    # the contribution of a node to itself is 0
    rank_list = [(node, 0)]
    if len(outgoing_links) > 0:
        mass_to_send = rank / len(outgoing_links)
        for link in outgoing_links:
            rank_list.append((link, mass_to_send))
    return rank_list


if __name__ == "__main__":
    if len(sys.argv) != 4:
        # args: [1]=input_file(txt),   [2]=output_file(txt),    [3]=nIter
        print("Usage: page_rank.py <input_file> <output> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # import context from Spark (distributed computing using yarn, name of the application)
    conf = SparkConf().setMaster("yarn").setAppName("page_rank_baggins").set("spark.dynamicAllocation.enabled", True)
    sc = SparkContext(conf=conf)

    # import input data from txt file to rdd
    input_data_rdd = sc.textFile(sys.argv[1])

    DAMPING_FACTOR_BR = sc.broadcast(DAMPING_FACTOR)

    # count number of nodes in the input dataset, broadcast the value (equal for each worker)
    node_number = input_data_rdd.count()
    node_number_br = sc.broadcast(node_number)

    # parse input rdd to get graph structure (k=title, v=[outgoing links])
    nodes = input_data_rdd.map(lambda input_line: data_parser(input_line)).cache()

    considered_keys = nodes.keys().collect()

    # set the initial pagerank (1/node_number)
    page_ranks = nodes.mapValues(lambda value: 1 / node_number_br.value)

    for i in range(int(sys.argv[3])):
        # computes masses to send (node_tuple[0] = title | node_tuple[1][0] = outgoing_links | node_tuple[1][1] = rank)
        contribution_list = nodes.join(page_ranks) \
            .flatMap(lambda node_tuple: spread_rank(node_tuple[0], node_tuple[1][0], node_tuple[1][1]))

        # inner join to consider only nodes inside the considered network
        considered_contributions = contribution_list.filter(lambda x: x[0] in considered_keys)

        # aggregate contributions for each node, compute final ranks
        page_ranks = considered_contributions.reduceByKey(lambda x, y: x + y) \
            .mapValues(lambda summed_contributions: (float(1 - DAMPING_FACTOR_BR.value) / node_number) +
                                                    (DAMPING_FACTOR_BR.value * float(summed_contributions)))

    # swap key and value, sort by key (by pagerank) and swap again
    sorted_page_ranks = page_ranks.map(lambda a: (a[1], a[0])) \
        .sortByKey(False) \
        .map(lambda a: (a[1], a[0]))

    # save the output
    sorted_page_ranks.saveAsTextFile(sys.argv[2])
