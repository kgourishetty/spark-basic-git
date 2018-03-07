package com.sparktesting.twitternlp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.*;

import com.sparktesting.model.HealthDataModel;
import com.twitter.opennlp.sentimemt.NLP;

import scala.Tuple2;

public class SparkTwitterNlp {

	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Twitter Sentiment");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFile);

		JavaRDD<String> rdd_records = data.map(new Function<String, String>() {
			public String call(String line) throws Exception {
				

				return line.toString();

			}
		});

		JavaRDD<String> en_records = rdd_records.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String json) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jObject = new JSONObject(json.toString()); // json

				String lang = jObject.getString("lang");
				if (lang.equalsIgnoreCase("en")) {
					return true;
				}

				return false;
			}
		});

		

		System.out.println("count of records =  = " + en_records.count());
		NLP.init();
		JavaRDD<String> sentimentdata = en_records.map(new Function<String, String>() {

			@Override
			public String call(String json) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jObject = new JSONObject(json.toString()); // json

				String tweetText = jObject.getString("text");
				int score = NLP.findSentiment(tweetText);
				String value = tweetText.trim() + "," + score + "\n";
				return value;
			}
		});

		sentimentdata.saveAsTextFile(outputFile);

	}

}
