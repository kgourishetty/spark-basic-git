package com.sparktesting.twitternlp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.*;

import com.sparktesting.model.HealthDataModel;
import com.sparktesting.model.TweetData;
import com.twitter.opennlp.sentimemt.NLP;

import scala.Tuple2;

public class SparkTwitterNlpSearch {

	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];
		final String keyword = args[2];

		SparkConf conf = new SparkConf().setAppName("Twitter Sentiment");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFile);
		/* Reading file into String RDD */
		JavaRDD<String> rdd_records = data.map(new Function<String, String>() {
			public String call(String line) throws Exception {
				// Here you can use JSON
				// Gson gson = new Gson();
				// gson.fromJson(line, Record.class);

				return line.toString();

			}
		});
		/* Creating New RDD by filtering the language of tweet */
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

		/* Creating New RDD by filtering the tweet with given search keyword */
		JavaRDD<String> search_records = en_records.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String json) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jObject = new JSONObject(json.toString()); // json

				String tweet = jObject.getString("text");
				if (tweet.toLowerCase().contains(keyword.toLowerCase())) {
					return true;
				}

				return false;
			}
		});

		System.out.println("count of search_records =  = " + search_records.count());
		NLP.init();
		/*
		 * Creating New RDD by taking search result as input and genarting the
		 * score
		 */
		JavaRDD<TweetData> sentimentdata = search_records.map(new Function<String, TweetData>() {

			@Override
			public TweetData call(String json) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jObject = new JSONObject(json.toString()); // json

				String tweetText = jObject.getString("text");
				int score = NLP.findSentiment(tweetText);
				String lang = jObject.getString("lang");
				String value = tweetText.trim() + "," + score + "\n";
				TweetData td = new TweetData();
				td.setTweet(tweetText);
				td.setScore(score);
				td.setLang(lang);
				return td;
			}
		});
		/* Creating New RDD for positive tweets */
		JavaRDD<TweetData> positivedata = sentimentdata.filter(new Function<TweetData, Boolean>() {

			@Override
			public Boolean call(TweetData td) throws Exception {
				// TODO Auto-generated method stub
				// json

				if (td.getScore() >= 2) {
					return true;
				}

				return false;
			}
		});
		/* Creating New RDD for negative tweets */
		JavaRDD<TweetData> negativedata = sentimentdata.filter(new Function<TweetData, Boolean>() {

			@Override
			public Boolean call(TweetData td) throws Exception {
				// TODO Auto-generated method stub
				// json

				if (td.getScore() < 2) {
					return true;
				}

				return false;
			}
		});

		System.out.println("positive tweets count = " + positivedata.count());
		System.out.println("negative tweets count = " + negativedata.count());

		sentimentdata.saveAsTextFile(outputFile);

	}

}
