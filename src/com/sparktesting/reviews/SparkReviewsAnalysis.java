package com.sparktesting.reviews;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkReviewsAnalysis {
	
	public static void main(String[] args) {
		
		
		///user/cloudera/Datasets/Reviews/complete.json.gz
		
		
		

		SparkConf conf = new SparkConf().setAppName("Twitter Sentiment");
		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile("/user/cloudera/datasets/review_meta/metadata.json.gz");
		
		data = data.repartition(50);
		
		System.out.println(data.count());
	}

}
