package com.wood.rdd;

import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;

import scala.Tuple2;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String inputFile = "/home/user/workspace/DumpLinkedinData/commonio/result/pdiDump/pdiDump.txt";
		String outputFile = "/home/user/workspace/SparkResearch/output";
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		//JavaRDD<String> inputWithPartition = sc.textFile(inputFile, 1);
		// Split up into words.
		//input.foreach(f -> System.out.println("datas::" + f));

		
		JavaRDD<String> words = input
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(" "));
					}
				});
		words.foreach(f -> System.out.println("Words: " + f));

		// Transform into word and count.
		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});
		pairs.foreach(f -> System.out.println("Pairs:: " + f));

		// action
		JavaPairRDD<String, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				});
		
		counts.foreach(f -> System.out.println("counts Pair: " + f));
		// Save the word count back out to a text file, causing evaluation.
		//action
		counts.saveAsTextFile(outputFile);
	}
}