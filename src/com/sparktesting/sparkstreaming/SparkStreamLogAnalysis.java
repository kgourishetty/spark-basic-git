package com.sparktesting.sparkstreaming;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamLogAnalysis {

	
	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];
		String keyword = args[2];

		SparkConf conf = new SparkConf().setAppName("Log Analysis");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(60));

		JavaDStream<String> trainingData = jssc.textFileStream(inputFile);
		
		System.out.println("///////////////////////////////////////////////////////");
		
		trainingData.print();

		System.out.println("#################################################################");
		JavaDStream<String> errorStream = trainingData.map(new Function<String, String>() {

			@Override
			public String call(String line) throws Exception {
				// TODO Auto-generated method stub
				if (line.toLowerCase().contains("info")) {
					return line;
				}
				return null;
			}
		});
		
		
		
		
		

		jssc.start();
		jssc.awaitTermination();

	}

}
