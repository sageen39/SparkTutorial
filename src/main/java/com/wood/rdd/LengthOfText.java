package com.wood.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LengthOfText {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc
				.textFile("/home/user/workspace/SparkResearch/wordLength.txt");

		// action
		System.out.println("Total Lines: " + lines.count());
		System.out.println("First Line: " + lines.first());
		System.out.println("upto Element: " + lines.take(2));

		// transformation
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

		lineLengths.foreach(f -> System.out.println("---> " + f));

		// action
		int totalLength = lineLengths.reduce((a, b) -> a + b);

		System.out.println("Total Length:: "+totalLength);
	}

}
