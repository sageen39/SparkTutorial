package com.wood.datasets;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.DataFrame;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

public class ProgrammaticalSchema {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Person").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// sc is an existing JavaSparkContext.
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<String> people = sc.textFile("people.txt");
		//DataFrame df = sqlContext.read().json("people.json");

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      String[] fields = record.split(",");
		      return RowFactory.create(fields[0], fields[1].trim());
		    }
		  });

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		// Register the DataFrame as a table.
		peopleDataFrame.registerTempTable("people");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT * FROM people");

		results.show();
	}

}
