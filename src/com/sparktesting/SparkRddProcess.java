package com.sparktesting;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.sparktesting.model.HealthDataModel;

public class SparkRddProcess {

	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Csv Reader");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFile);

		final int i = 0;
		JavaRDD<HealthDataModel> rdd_records = data.map(new Function<String, HealthDataModel>() {
			public HealthDataModel call(String line) throws Exception {
				// Here you can use JSON
				// Gson gson = new Gson();
				// gson.fromJson(line, Record.class);
				HealthDataModel sd = new HealthDataModel();

				try {

					String[] fields = line.split(",");

					sd.setEducationLevel(fields[0]);
					sd.setAge(Long.parseLong(fields[1].trim()));
					sd.setAgeRange(fields[2]);
					sd.setEmploymentStatus(fields[3]);
					sd.setGender(fields[4]);
					sd.setChildren(Long.parseLong(fields[5].trim()));
					sd.setWeeklyEarnings(Long.parseLong(fields[6].trim()));
					sd.setYear(Long.parseLong(fields[7].trim()));
					sd.setWeeklyHoursWorked(Long.parseLong(fields[8].trim()));
					sd.setSleeping(Long.parseLong(fields[9].trim()));
					sd.setGrooming(Long.parseLong(fields[10].trim()));
					sd.setHousework(Long.parseLong(fields[11].trim()));
					sd.setFoodDrinkPrep(Long.parseLong(fields[12].trim()));
					sd.setCaringforChildren(Long.parseLong(fields[13].trim()));
					sd.setPlayingwithChildren(Long.parseLong(fields[14].trim()));
					sd.setJobSearching(Long.parseLong(fields[15].trim()));
					sd.setShopping(Long.parseLong(fields[16].trim()));
					sd.setEatingandDrinking(Long.parseLong(fields[17].trim()));
					sd.setSocializingRelaxing(Long.parseLong(fields[18].trim()));
					sd.setTelevision(Long.parseLong(fields[19].trim()));
					sd.setGolfing(Long.parseLong(fields[20].trim()));
					sd.setRunning(Long.parseLong(fields[21].trim()));
					sd.setVolunteering(Long.parseLong(fields[22].trim()));
				} catch (Exception e) {
					e.printStackTrace();
				}
				return sd;
			}
		});
		
		//rdd_records.repartition(numPartitions)
		/*
		 * JavaPairRDD<String, HealthDataModel> gender_records =
		 * rdd_records.mapToPair(new PairFunction<HealthDataModel, String,
		 * HealthDataModel>() {
		 * 
		 * @Override public Tuple2<String, HealthDataModel> call(HealthDataModel
		 * arg0) throws Exception { // TODO Auto-generated method stub return
		 * null; }
		 * 
		 * });
		 */

		JavaRDD<HealthDataModel> gender_records = rdd_records.filter(new Function<HealthDataModel, Boolean>() {

			@Override
			public Boolean call(HealthDataModel hd) throws Exception {
				// TODO Auto-generated method stub
				if (hd.getGender().equalsIgnoreCase("male")) {
					if (hd.getAge() >= 20 && hd.getAge() <= 25) {

						if (hd.getYear() == 2007 || hd.getYear() == 2008) {
							return true;
						}

					}

				}
				return false;
			}
		});

		final JavaRDD<HealthDataModel> final_records = gender_records.sortBy(new Function<HealthDataModel, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(HealthDataModel value) throws Exception {
				return value.getGolfing();
			}
		}, false, 1);

		
		  List<HealthDataModel> final_records2 = final_records.top(5);
		  
		  for(HealthDataModel hd:final_records2) {
		  System.out.println(hd.getEducationLevel()+"--------"+hd.getGolfing())
		  ; } System.out.println("gender count=-------------------------------"
		  +final_records2.size());;
		

		JavaRDD<String> frdd = gender_records.map(new Function<HealthDataModel, String>() {

			@Override
			public String call(HealthDataModel hd) throws Exception {

				String line = hd.getEducationLevel() + "," + hd.getAge() + "," + hd.getAgeRange() + ","
						+ hd.getEmploymentStatus() + "," + hd.getGender() + "," + hd.getChildren() + ","
						+ hd.getWeeklyEarnings() + "," + hd.getYear() + "," + hd.getWeeklyHoursWorked() + ","
						+ hd.getSleeping() + "," + hd.getGrooming() + "," + hd.getHousework() + ","
						+ hd.getFoodDrinkPrep() + "," + hd.getCaringforChildren() + "," + hd.getPlayingwithChildren()
						+ "," + hd.getJobSearching() + "," + hd.getShopping() + "," + hd.getEatingandDrinking() + ","
						+ hd.getSocializingRelaxing() + "," + hd.getTelevision() + "," + hd.getGolfing() + ","
						+ hd.getRunning() + "," + hd.getVolunteering();
				return line;
			}
		});
		
		//frdd.saveAsTextFile(outputFile);
		frdd.saveAsObjectFile(outputFile);
		
	}
}