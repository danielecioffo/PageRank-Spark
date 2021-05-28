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
        SparkConf sparkConf = new SparkConf().setAppName("pageRankJava").setMaster("yarn");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputDataRDD = javaSparkContext.textFile(args[0], 2);

        Broadcast<Double> DUMPING_FACTOR_BR = javaSparkContext.broadcast(DUMPING_FACTOR);

        // Count number of nodes (lines in the input file)
        long nodesNumber = inputDataRDD.count();

        JavaPairRDD<String, ArrayList<String>> nodesRDD = inputDataRDD.mapToPair(DataParser::parseData).cache();

        JavaPairRDD<String, Double> pageRankRDD = nodesRDD.mapValues(value -> 1 / (double) nodesNumber);

        List<String> consideredKeys = nodesRDD.keys().collect();

        for(int i = 0; i< Integer.parseInt(args[2]); i++) {
            JavaPairRDD<String, Double> contributionRDD = nodesRDD.join(pageRankRDD).flatMapToPair(node_tuple -> spreadRank(node_tuple._1, node_tuple._2._1, node_tuple._2._2));

            JavaPairRDD<String, Double> consideredContributionsRDD = contributionRDD.filter(x -> consideredKeys.contains(x._1));

            pageRankRDD = consideredContributionsRDD.reduceByKey(Double::sum)
                    .mapValues(summedContributions -> (1 - DUMPING_FACTOR_BR.value()) / nodesNumber + DUMPING_FACTOR_BR.value() * summedContributions);
        }

        JavaRDD<Tuple2<String, Double>> sortedPageRankRDD = pageRankRDD.map(pageRank -> pageRank).sortBy(page -> page._2, false, 12);
        sortedPageRankRDD.saveAsTextFile(args[1]);
    }

    private static Iterator<Tuple2<String, Double>> spreadRank(String title, ArrayList<String> outgoingLinks, Double pageRank) {
        ArrayList<Tuple2<String, Double>> result = new ArrayList<>();
        result.add(new Tuple2<>(title, 0.0));
        if(outgoingLinks.size() > 0) {
            double massToSend = pageRank/outgoingLinks.size();
            for(String link: outgoingLinks) {
                result.add(new Tuple2<>(link, massToSend));
            }
        }

        return result.iterator();
    }
}
