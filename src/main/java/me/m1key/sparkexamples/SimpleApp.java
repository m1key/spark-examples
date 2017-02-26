package me.m1key.sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

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

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        System.out.println(distData.reduce((a, b) -> a + b));

        sc.stop();
    }

}
