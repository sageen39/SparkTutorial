package com.wood.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
//Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.DataFrame;
//Import Row.
import org.apache.spark.sql.Row;
//Import RowFactory.
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

public class DataFrameEG {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Person").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame df = sqlContext.read().json("people.json");
		
		// Show the content of the DataFrame
		df.show();
		// age  name
		// null Michael
		// 30   Andy
		// 19   Justin

		// Print the schema in a tree format
		df.printSchema();
		// root
		// |-- age: long (nullable = true)
		// |-- name: string (nullable = true)
		
		//Remove duplicates
		df.dropDuplicates().show();

		// Select only the "name" column
		//df.select("name").show();
		// name
		// Michael
		// Andy
		// Justin

		// Select everybody, but increment the age by 1
		df.select(df.col("name"), df.col("age").plus(1)).show();
		// name    (age + 1)
		// Michael null
		// Andy    31
		// Justin  20

		// Select people older than 21
		df.filter(df.col("age").gt(21)).show();
		// age name
		// 30  Andy

		// Count people by age
		df.groupBy("age").count().show();
		// age  count
		// null 1
		// 19   1
		// 30   1
		
		//filtering the row having age null
		DataFrame dfFilterAgeNotNull = sqlContext.read().json("people.json").filter("isnotnull(age)");
		//creating people table
		dfFilterAgeNotNull.registerTempTable("people");
		DataFrame newdf = sqlContext.sql("select * from people");
		newdf.show();
	}

}
