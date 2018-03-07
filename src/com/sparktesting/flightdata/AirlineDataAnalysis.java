package com.sparktesting.flightdata;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class AirlineDataAnalysis {
	
	public static void main(String[] args) {
		
		String inputFile = args[0];
		

		SparkConf conf = new SparkConf().setAppName("Csv Reader 1").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFile);
		
		String header = data.first();
		
		data = data.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String line) throws Exception {
				if(!line.equalsIgnoreCase(header))
				{
					return true;
				}
				return false;
			}
		});
		
		JavaRDD<String[]> slipt_data = data.map(new Function<String, String[]>() {

			@Override
			public String[] call(String line) throws Exception {
				// TODO Auto-generated method stub
				return line.split(",");
			}
		}).filter(new Function<String[], Boolean>() {
			
			@Override
			public Boolean call(String[] strArray) throws Exception {
				// TODO Auto-generated method stub
				if(strArray[9].equalsIgnoreCase("NA"))
				{
					return false;
				}
				return true;
			}
		});
		
		JavaRDD<String[]> slipt_update = slipt_data.map(new Function<String[], String[]>() {

			@Override
			public String[] call(String[] strArray) throws Exception {
				
				List<String> dataList = new ArrayList<String>();
				// TODO Auto-generated method stub
				for(String s:strArray)
				{
					s = s.replace("NA", "0");
					dataList.add(s);
				}
				return  dataList.toArray(new String[dataList.size()]);
			}
		});
		
		JavaPairRDD<String, String[]> pair_rdd = slipt_update.mapToPair(new PairFunction<String[], String, String[]>() {

			@Override
			public Tuple2<String, String[]> call(String[] strArray) throws Exception {
				
				return new Tuple2<String, String[]>(strArray[9],strArray);
			}
		});
		
		
		
		JavaPairRDD<String, String[]> pair_rdd1 = pair_rdd.reduceByKey(new Function2<String[], String[], String[]>() {
			
			@Override
			public String[] call(String[] data1, String[] data2) throws Exception {
				
				List<String> dataList = new ArrayList<String>();
				dataList.add(data1[9]);
				dataList.add( Long.toString((Long.parseLong(data1[14])+Long.parseLong(data2[14]))));
				dataList.add(Long.toString((Long.parseLong(data1[15])+Long.parseLong(data2[15]))));
				dataList.add(Long.toString((Long.parseLong(data1[24])+Long.parseLong(data2[24]))));
				dataList.add(Long.toString((Long.parseLong(data1[25])+Long.parseLong(data2[25]))));
				dataList.add(Long.toString((Long.parseLong(data1[26])+Long.parseLong(data2[26]))) );
				dataList.add(Long.toString((Long.parseLong(data1[27])+Long.parseLong(data2[27]))));
				dataList.add(Long.toString((Long.parseLong(data1[28])+Long.parseLong(data2[28]))));
				dataList.add(data1[16]);
				dataList.add(data1[17]);
				
				return dataList.toArray(new String[dataList.size()]);
			}
		});
		
		
		
		System.out.println(pair_rdd1.count());
		List<Tuple2<String, String[]>> strList = pair_rdd1.take(10);
		for(Tuple2<String, String[]> strarray:strList)
		{
			System.out.println(strarray._2.length);
			System.out.println(Arrays.toString(strarray._2));
		}
		
		
	}

}
