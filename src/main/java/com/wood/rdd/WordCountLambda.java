package com.wood.rdd;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple1;
import scala.Tuple2;

public class WordCountLambda {

	public static void main(String[] args) {
		
		
		String inputFile = "/home/user/workspace/SparkResearch/wordLength.txt";
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
	    // Load our input data.
	    JavaRDD<String> lines = sc.textFile(inputFile);
	    
	    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	        public Iterable<String> call(String x) 
	        {
	          return Arrays.asList(x.split(" "));
	        }
	        });

	    JavaPairRDD<String, Integer> pairs = words.mapToPair(f->new Tuple2(f, 1));
	    
	  pairs.foreach(f->System.out.println("Pairs: "+f));
	  
	  JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
	  counts.foreach(f->System.out.println("pairs: "+f));
	}

}

//file = spark.textFile("hdfs://...") // open text file each element of the RDD is one line of the file
//counts = file.flatMap(lambda line: line.split(" ")) //flatMap is needed here to return every word (separated by a space) in the line as an Array
//             .map(lambda word: (word, 1)) //map each word to a value of 1 so they can be summed
//             .reduceByKey(lambda a, b: a + b) // get an RDD of the count of every unique word by aggregating (adding up) all the 1's you wrote in the last step
//counts.saveAsTextFile("hdfs://...") //Save the file onto HDFS
