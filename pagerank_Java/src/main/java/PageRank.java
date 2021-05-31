import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PageRank {
    private static final double DUMPING_FACTOR = 0.8;

    public static void main(String[] args) {
        // import context from Spark (distributed computing using yarn, set name of the application)
        SparkConf sparkConf = new SparkConf().setAppName("pageRankJava").setMaster("yarn");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // import input data from txt file to rdd
        JavaRDD<String> inputDataRDD = javaSparkContext.textFile(args[0]);

        // count number of nodes in the input dataset (the N number)
        long nodesNumber = inputDataRDD.count();

        // parse input rdd to get graph structure (k=title, v=[outgoing links])
        // the result is cached since it is static and to be accessible by every worker
        JavaPairRDD<String, ArrayList<String>> nodesRDD = inputDataRDD.mapToPair(DataParser::parseData).cache();

        // set the initial pagerank (1/node_number)
        JavaPairRDD<String, Double> pageRankRDD = nodesRDD.mapValues(value -> 1 / (double) nodesNumber);

        // list of title of the considered nodes. Explicitly computed to avoid the additional join
        List<String> consideredKeys = nodesRDD.keys().collect();

        for(int i = 0; i< Integer.parseInt(args[2]); i++) {
            // computes masses to send (node_tuple._1 = title | node_tuple._2._1 = outgoing_links | node_tuple._2._2 = rank)
            JavaPairRDD<String, Double> contributionRDD = nodesRDD.join(pageRankRDD).flatMapToPair(node_tuple -> spreadRank(node_tuple._1, node_tuple._2._1, node_tuple._2._2));

            // take the only contributions that are relative to considered nodes
            JavaPairRDD<String, Double> consideredContributionsRDD = contributionRDD.filter(x -> consideredKeys.contains(x._1));

            // aggregate contributions for each node, compute final ranks
            pageRankRDD = consideredContributionsRDD.reduceByKey(Double::sum)
                    .mapValues(summedContributions -> (1 - DUMPING_FACTOR) / nodesNumber + DUMPING_FACTOR * summedContributions);
        }

        // sort by value (pagerank)
        JavaRDD<Tuple2<String, Double>> sortedPageRankRDD = pageRankRDD.map(pageRank -> pageRank).sortBy(page -> page._2, false, 1);
        // save the output
        sortedPageRankRDD.saveAsTextFile(args[1]);
    }

    private static Iterator<Tuple2<String, Double>> spreadRank(String title, ArrayList<String> outgoingLinks, Double pageRank) {
        ArrayList<Tuple2<String, Double>> result = new ArrayList<>();
        // the contribution of a node to itself is 0
        result.add(new Tuple2<>(title, 0.0));
        // for any other outgoing link, this nodes contributes by rank/N, so we save (link-contribution) pairs
        if(outgoingLinks.size() > 0) {
            double massToSend = pageRank/outgoingLinks.size();
            for(String link: outgoingLinks) {
                result.add(new Tuple2<>(link, massToSend));
            }
        }

        return result.iterator();
    }
}
