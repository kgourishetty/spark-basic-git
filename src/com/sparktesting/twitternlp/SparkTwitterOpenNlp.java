package com.sparktesting.twitternlp;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.json.JSONObject;

import com.twitter.opennlp.sentimemt.NLP;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.doccat.DocumentSampleStream;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import java.io.Serializable;
import scala.collection.generic.BitOperations.Int;

public class SparkTwitterOpenNlp implements Serializable {
	 private DoccatModel model;
	 
	 

	public DoccatModel getModel() {
		return model;
	}

	public void setModel(DoccatModel model) {
		this.model = model;
	}

	public static void main(String[] args) {

		final SparkTwitterOpenNlp twitterCategorizer = new SparkTwitterOpenNlp();
		twitterCategorizer.trainModel();

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Twitter Sentiment");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.addJar("/home/cloudera/share/sparkWindev.jar");
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
				
				
				
				long result1 = twitterCategorizer.classifyNewTweet(tweetText.toString());
				String  value = tweetText + "---------------" +  Long.toString(result1);
				return value;
			}
		});
		
		sentimentdata.saveAsTextFile(outputFile);
	}

	public void trainModel() {
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
	}

	public  int classifyNewTweet(String tweet) throws IOException {
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
