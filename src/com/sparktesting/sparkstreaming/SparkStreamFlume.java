package com.sparktesting.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;

import org.apache.log4j.*;

public class SparkStreamFlume {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// Configure and initialize the SparkStreamingContext
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("VerySimpleStreamingApp");

		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(120));

		InputStream inputStream;
		Path pt = new Path("hdfs:///user/cloudera/mypropsfile.conf");
		FileSystem fs = FileSystem.get(streamingContext.sparkContext().hadoopConfiguration());
		inputStream = fs.open(pt);

		Properties properties = new Properties();
		properties.load(inputStream);

		Logger.getRootLogger().setLevel(Level.ERROR);

		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String HOST = properties.getProperty("input.host");
		int PORT = Integer.parseInt(properties.getProperty("input.port"));
		String type = properties.getProperty("log.type");
		String keyword = properties.getProperty("search.keyword");
		String startTime = properties.getProperty("log.start.time");
		String endTime = properties.getProperty("log.end.time");
		String outpath = properties.getProperty("output.path");
		try {
			Date logdate = sdfDate.parse(startTime);
			final long startMilli = logdate.getTime();
			logdate = sdfDate.parse(endTime);
			final long endMilli = logdate.getTime();

			System.out.println("startMilli \t" + startMilli);
			System.out.println("endMilli \t" + endMilli);

			// Receive streaming data from the source
			JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);

			lines.print();

			JavaDStream<String> filterType = lines.filter(new Function<String, Boolean>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String line) throws Exception {
					// TODO Auto-generated method stub
					if (line.toLowerCase().contains(type.toLowerCase())) {
						return true;
					}
					return false;
				}
			}).filter(new Function<String, Boolean>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String line) throws Exception {
					// TODO Auto-generated method stub
					String[] elements = line.split("\t");
					SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date logdate = sdfDate.parse(elements[0]);
					long logTime = logdate.getTime();
					if (logTime > startMilli && logTime < endMilli) {
						return true;
					}

					return false;
				}
			}).filter(new Function<String, Boolean>() {

				@Override
				public Boolean call(String line) throws Exception {
					// TODO Auto-generated method stub
					if (line.toLowerCase().contains(keyword.toLowerCase())) {
						return true;
					}
					return false;
				}
			});
			;

			filterType.foreachRDD(new Function<JavaRDD<String>, Void>() {

				@Override
				public Void call(JavaRDD<String> rdd) throws Exception {
					Date now = new Date();
					System.out.println("#############################################");
					System.out.println(" count after filter" + rdd.count());
					FSDataOutputStream fileOutputStream = null;
					try {
						Path hdfsPath = new Path(outpath+"loganalysis.out");

						if (fs.exists(hdfsPath)) {
							fileOutputStream = fs.append(hdfsPath);

						} else {
							fileOutputStream = fs.create(hdfsPath);

						}

						List<String> ls = rdd.collect();
						for (String l : ls) {
							fileOutputStream.writeBytes(l + "\n");
						}
					} finally {
						
						if (fileOutputStream != null) {
							fileOutputStream.close();
						}
					}
					

					return null;
				}
			});

			// Execute the Spark workflow defined above
			streamingContext.start();
			streamingContext.awaitTermination();
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

}
