package com.wood.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.wood.domain.Person;

public class ReflectionSchema {

	public static void main(String[] args) {


		SparkConf conf = new SparkConf().setAppName("Person").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = new SQLContext(sc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<Person> people = sc.textFile("/home/user/workspace/SparkResearch/people.txt").map(
		  new Function<String, Person>() {
		    public Person call(String line) throws Exception {
		      String[] parts = line.split(",");

		      Person person = new Person();
		      person.setName(parts[0]);
		      person.setAge(Integer.parseInt(parts[1].trim()));

		      return person;
		    }
		  });

		// Apply a schema to an RDD of JavaBeans and register it as a table.
		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("people");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 40");

		teenagers.show();
		
	}

}
