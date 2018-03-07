package com.sparktesting.twitternlp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.json.JSONObject;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSampleStream;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

public class SparkTwitterOpenNlp1 {

	 private static DoccatModel model;
	 
	 

	public static DoccatModel getModel() {
		return model;
	}

	public static void setModel(DoccatModel model) {
		model = model;
	}
	
	
	public static void main(String[] args) {

		InputStream dataIn = null;
		try {
			dataIn = new FileInputStream("/home/cloudera/share/sample_tweet.txt");
			ObjectStream lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
			ObjectStream sampleStream = new DocumentSampleStream(lineStream);
			// Specifies the minimum number of times a feature must be seen
			int cutoff = 2;
			int trainingIterations = 30;
			setModel(DocumentCategorizerME.train("en", sampleStream, cutoff, trainingIterations));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (dataIn != null) {
				try {
					dataIn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

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
		
		JavaRDD<String> sentimentdata = en_records.map(new Function<String, String>() {

			@Override
			public String call(String json) throws Exception {
				// TODO Auto-generated method stub
				JSONObject jObject = new JSONObject(json.toString()); // json

				String tweetText = jObject.getString("text");
				
				
				//long result1 = twitterCategorizer.classifyNewTweet(tweetText.toString());
				String  value = tweetText;// + "---------------" +  Long.toString(result1);
				return value;
			}
		});
		
		List<String> tweets = sentimentdata.collect();
		
		for(String tweet :tweets)
		{
			try {
				int result = classifyNewTweet(tweet);
				System.out.println("tweet====="+tweet);
				System.out.println("sentiment score ====="+result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		sentimentdata.saveAsTextFile(outputFile);
	}

	public void trainModel() {
		
	}

	public static  int classifyNewTweet(String tweet) throws IOException {
		DocumentCategorizerME myCategorizer = new DocumentCategorizerME(getModel());
		double[] outcomes = myCategorizer.categorize(tweet);
		String category = myCategorizer.getBestCategory(outcomes);

		
		if (category.equalsIgnoreCase("1")) {
			
			return 1;
		} else {
			
			return 0;
		}

	}



}
