package com.sparktesting.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

public class SparkSqlJsonDataFrame {
	
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		String keyword = args[2];
		
		
		SparkConf conf = new SparkConf().setAppName("Json data frame");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<String> data = sc.textFile(inputFile);
		
		DataFrame tweetJson = sqlContext.jsonRDD(data);
		//sqlContext.
		
		tweetJson.registerTempTable("tweetjsontable");
		
		tweetJson.printSchema();
		
		DataFrame tweetJsonEn =  tweetJson.filter(tweetJson.col("lang").equalTo("en"));
		
		System.out.println("count of total records = "+tweetJson.count());;
		System.out.println("count of english records = "+tweetJsonEn.count());;
		
		DataFrame tweetjsonSearch = tweetJsonEn.filter(tweetJson.col("text").contains(keyword));
		System.out.println("count of search records = "+tweetjsonSearch.count());
		
		
		DataFrame tweetString = tweetjsonSearch.select(tweetjsonSearch.col("text"));
		
		tweetJson.groupBy(tweetJson.col("lang")).agg(functions.count("text").as("count")).show();;
		
		
		RDD<Row> datatweet = tweetString.rdd();
		//datatweet.
		datatweet.saveAsTextFile(outputFile);
	}

}
