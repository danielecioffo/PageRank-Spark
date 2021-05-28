import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class PageRank {
    private static JavaSparkContext javaSparkContext;
    private static SparkConf sparkConf;
    private static final double DUMPING_FACTOR = 0.8;

    public static void main(String[] args) {
        sparkConf = new SparkConf().setAppName("pageRankJava").setMaster("yarn");
        javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputDataRDD = javaSparkContext.textFile(args[0]);

        Broadcast<Double> DUMPING_FACTOR_BR = javaSparkContext.broadcast(DUMPING_FACTOR);

        // Count number of nodes (lines in the input file)
        long nodesNumber = inputDataRDD.count();

        JavaRDD<Node> nodesRDD = inputDataRDD.map(inputLine -> DataParser.parseData(inputLine));




    }
}
