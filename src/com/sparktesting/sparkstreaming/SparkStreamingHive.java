package com.sparktesting.sparkstreaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.sparktesting.model.TwitterHiveModel;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class SparkStreamingHive {
	


	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTwitterHelloWorldExample");
		String[] jars = { "/home/cloudera/share/SparkProgram/lib/twitter4j-core-4.0.4.jar",
				"/home/cloudera/share/SparkProgram/lib/twitter4j-stream-4.0.4.jar",
				"/home/cloudera/share/twitterjars/spark-streaming-twitter_2.10-1.6.0-cdh5.10.0.jar" };
		conf.setJars(jars);
		//conf.set
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(120));
		SQLContext sqlContext =new HiveContext(jssc.sc());
		System.setProperty("twitter4j.oauth.consumerKey", "OT8zFJOz6hnlFmo8cR1StiXbS");
		System.setProperty("twitter4j.oauth.consumerSecret", "noW2AQkcmrdvGxJYCrOMnSEFNb9PFFLvKLQV4g7www7pLOPS1v");
		System.setProperty("twitter4j.oauth.accessToken", "554122868-ekZlUkgddUAAlaHK1vfwcQip6F5V50Z5RB75qQsV");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "eODR8jiHLqDI1qhJ99SiOmeuJUG2eVahNfCLM5wpbhzYG");
		System.setProperty("twitter4j.streamBaseURL", "https://stream.twitter.com/1.1/");
		System.setProperty("twitter4j.loggerFactory", "twitter4j.NullLoggerFactory");

		jssc.sc().setLogLevel("ERROR");

		String[] filters = new String[] {};
		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filters);

		
		
		
		JavaDStream<TwitterHiveModel> tags = twitterStream.filter(new Function<Status, Boolean>() {
			
			@Override
			public Boolean call(Status tweet) throws Exception {
				tweet.getCreatedAt().getTime();
				if(tweet.getLang().equalsIgnoreCase("en")){
					return true;
				}
				return false;
			}
		}).flatMap(new FlatMapFunction<Status, TwitterHiveModel>() {

			@Override
			public Iterable<TwitterHiveModel> call(Status s) throws Exception {
				
				List<TwitterHiveModel> returnlist = new ArrayList<TwitterHiveModel>();
				HashtagEntity[] hashs = s.getHashtagEntities();
				for(int i = 0;i<hashs.length;i++)
				{
					TwitterHiveModel hvm = new TwitterHiveModel();
					
					hvm.setTweettime(s.getCreatedAt().toString());
					hvm.setHastag(hashs[i].getText());
					hvm.setTweet(s.getText());
					returnlist.add(hvm);
				}
				return returnlist;
			}
		});
		
		
		
		tags.foreachRDD(new Function<JavaRDD<TwitterHiveModel>, Void>() {
			
			@Override
			public Void call(JavaRDD<TwitterHiveModel> arg0) throws Exception {
				// TODO Auto-generated method stub
				
				//sqlContext
				
				String sqlQuery = "INSERT INTO TABLE tweetdata  select tweettime,hastag,tweet from tweet_sparktable";
				
				DataFrame tweet_df = sqlContext.createDataFrame(arg0, TwitterHiveModel.class);
				//tweet_df.schema();
				tweet_df.printSchema();
				tweet_df.registerTempTable("tweet_sparktable");
				
				sqlContext.sql("use twitter");
				sqlContext.sql(sqlQuery);
				
				
				return null;
			}
		});
		

		tags.print();
		jssc.start();
		jssc.awaitTermination();
	}



}
