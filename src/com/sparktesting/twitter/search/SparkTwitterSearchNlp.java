package com.sparktesting.twitter.search;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.GeoLocation;
import twitter4j.Status;

import twitter4j.Status;

public class SparkTwitterSearchNlp {

	public static void main(String[] args) {
		
		/*String inputFile = args[0];
		String outputFile = args[1];
		String keyword = args[2];
		String[] keywords = null ;
		if(keyword.contains(","))
		{
			keywords  = keyword.split(",");
		}
		else
		{
			keywords[0]  = args[2];
		}
		
		long limit = Long.parseLong(args[3]);*/
		
		
		String[] keywords = {""};
		System.setProperty("twitter4j.oauth.consumerKey", "OT8zFJOz6hnlFmo8cR1StiXbS");
	    System.setProperty("twitter4j.oauth.consumerSecret", "noW2AQkcmrdvGxJYCrOMnSEFNb9PFFLvKLQV4g7www7pLOPS1v");
	    System.setProperty("twitter4j.oauth.accessToken", "554122868-ekZlUkgddUAAlaHK1vfwcQip6F5V50Z5RB75qQsV");
	    System.setProperty("twitter4j.oauth.accessTokenSecret", "eODR8jiHLqDI1qhJ99SiOmeuJUG2eVahNfCLM5wpbhzYG");
		
	    SparkConf conf = new SparkConf().setAppName("SparkTwitterHelloWorldExample");
	    
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));
        
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, keywords);
        
        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );
        statuses.print();
        jssc.start();

	}
	
}
