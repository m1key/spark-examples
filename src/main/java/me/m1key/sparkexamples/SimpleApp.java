package me.m1key.sparkexamples;

import lombok.SneakyThrows;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

import static com.google.common.io.Resources.getResource;

public class SimpleApp {
    public static void main(String[] args){
        String logFile = "/home/mike/Development/Libraries/spark-2.1.0-bin-hadoop2.7/README.md";

        SparkConf conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]");;
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(word -> word.contains("a")).count();
        long numBs = logData.filter(word -> word.contains("b")).count();

        System.out.println("As: " + numAs);
        System.out.println("Bs: " + numBs);

        List<Integer> intData = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(intData);

        System.out.println("Sum: " + distData.reduce((a, b) -> a + b));
        System.out.println("Count: " + distData.count());

        JavaRDD<String[]> data = sc.textFile(getResourceUrl("UserPurchaseHistory.csv"))
                .map(s -> s.split(","));
        long numPurchases = data.count();
        long uniqueUsers = data.map(s -> s[0]).distinct().count();
        double totalRevenue = data.map(s -> Double.parseDouble(s[2])).reduce((a , b) -> a + b);

        List<Tuple2<String, Integer>> pairs = data
                .mapToPair(
                        (PairFunction<String[], String, Integer>) strings -> new Tuple2(strings[1], 1))
                .reduceByKey(
                        (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2)
                .collect();

        (new ArrayList<>(pairs)).sort((o1, o2) -> -(o1._2() - o2._2()));

        String mostPopular = pairs.get(0)._1();
        int purchases = pairs.get(0)._2();

        System.out.println("Total purchases: " + numPurchases);
        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total revenue: " + totalRevenue);
        System.out.println(String.format("Most popular product: %s with %d purchases",
                mostPopular, purchases));


        System.out.println(totalRevenue);

        sc.stop();
    }

    @SneakyThrows
    public static String getResourceUrl(String path) {
        return getResource(path).toURI().getPath();
    }

}
