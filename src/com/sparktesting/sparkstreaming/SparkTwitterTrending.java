package com.sparktesting.sparkstreaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class SparkTwitterTrending {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTwitterHelloWorldExample");
		/*String[] jars = { "/home/cloudera/share/SparkProgram/lib/twitter4j-core-4.0.4.jar",
				"/home/cloudera/share/SparkProgram/lib/twitter4j-stream-4.0.4.jar",
				"/home/cloudera/share/twitterjars/spark-streaming-twitter_2.10-1.6.0-cdh5.10.0.jar" };*/
		//conf.setJars(jars);
		//conf.set
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		System.setProperty("twitter4j.oauth.consumerKey", "OT8zFJOz6hnlFmo8cR1StiXbS");
		System.setProperty("twitter4j.oauth.consumerSecret", "noW2AQkcmrdvGxJYCrOMnSEFNb9PFFLvKLQV4g7www7pLOPS1v");
		System.setProperty("twitter4j.oauth.accessToken", "554122868-ekZlUkgddUAAlaHK1vfwcQip6F5V50Z5RB75qQsV");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "eODR8jiHLqDI1qhJ99SiOmeuJUG2eVahNfCLM5wpbhzYG");
		System.setProperty("twitter4j.streamBaseURL", "https://stream.twitter.com/1.1/");
		System.setProperty("twitter4j.loggerFactory", "twitter4j.NullLoggerFactory");

		jssc.sc().setLogLevel("ERROR");

		String[] filters = new String[] {};
		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filters);

		// Without filter: Output text of all tweets
		/*JavaDStream<String> tags = twitterStream.filter(new Function<Status, Boolean>() {

			@Override
			public Boolean call(Status status) throws Exception {
				// TODO Auto-generated method stub
				HashtagEntity[] htags = status.getHashtagEntities();
				if (htags.length > 0) {
					return true;
				}
				return false;
			}
		}).filter(new Function<Status, Boolean>() {
			
			@Override
			public Boolean call(Status tweet) throws Exception {
				if(tweet.getLang().equalsIgnoreCase("en")){
					return true;
				}
				return false;
			}
		}).map(new Function<Status, String>() {

			@Override
			public String call(Status status) throws Exception {
				// TODO Auto-generated method stub

				HashtagEntity[] htags = status.getHashtagEntities();
				if (htags.length > 0) {
					return htags[0].getText();
				}

				return null;
			}
		});*/
		
		
		JavaDStream<String> tags = twitterStream.filter(new Function<Status, Boolean>() {
			
			@Override
			public Boolean call(Status tweet) throws Exception {
				tweet.getCreatedAt().getTime();
				if(tweet.getLang().equalsIgnoreCase("en")){
					return true;
				}
				return false;
			}
		}).flatMap(new FlatMapFunction<Status, String>() {

			@Override
			public Iterable<String> call(Status s) throws Exception {
				
				return Arrays.asList(s.getText().split(" "));
			}
		}).filter(new Function<String, Boolean>() {
		      @Override
		      public Boolean call(String word) throws Exception {
		        return word.startsWith("#");
		      }
		    });;

		JavaPairDStream<String, Integer> hashTagCount = tags.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				// leave out the # character
				return new Tuple2<>(s.substring(1), 1);
			}
		});

		JavaPairDStream<String, Integer> hashTagTotals = hashTagCount
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, Durations.seconds(60));

		
		JavaPairDStream<Integer, String> hashTagkeys = hashTagTotals.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> keypair) throws Exception {
				
				return new Tuple2<Integer, String>(keypair._2, keypair._1);
			}
		});
		JavaPairDStream<Integer, String> hashTagkeys1 = hashTagkeys.transformToPair(new Function<JavaPairRDD<Integer,String>, JavaPairRDD<Integer,String>>() {

			@Override
			public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> keypair) throws Exception {
				
				return keypair.sortByKey(false);
			}
		});
		
		//hashTagkeys1.save

		hashTagkeys1.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {

			@Override
			public Void call(JavaPairRDD<Integer, String> topicPairs) throws Exception {
				List<Tuple2<Integer, String>> topList = topicPairs.take(20);
				System.out
						.println(String.format("\nTrending topics in last 10 minutes (%s total):", topicPairs.count()));
				for (Tuple2<Integer, String> pair : topList) {
					System.out.println(String.format("%s (%s times)", pair._2, pair._1));
				}
				return null;
			}
		});

		tags.print();
		jssc.start();
		jssc.awaitTermination();
	}

}
