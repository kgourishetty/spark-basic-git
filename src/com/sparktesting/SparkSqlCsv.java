package com.sparktesting;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import com.sparktesting.model.HealthDataModel;

public class SparkSqlCsv {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];
		String sqlQuery = args[2];

		SparkConf conf = new SparkConf().setAppName("Csv Reader");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(inputFile);
		SQLContext sqlContext = new SQLContext(sc);
		final int i = 0;
		JavaRDD<HealthDataModel> rdd_records = data
				.map(new Function<String, HealthDataModel>() {
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
							sd.setWeeklyEarnings(Long.parseLong(fields[6]
									.trim()));
							sd.setYear(Long.parseLong(fields[7].trim()));
							sd.setWeeklyHoursWorked(Long.parseLong(fields[8]
									.trim()));
							sd.setSleeping(Long.parseLong(fields[9].trim()));
							sd.setGrooming(Long.parseLong(fields[10].trim()));
							sd.setHousework(Long.parseLong(fields[11].trim()));
							sd.setFoodDrinkPrep(Long.parseLong(fields[12]
									.trim()));
							sd.setCaringforChildren(Long.parseLong(fields[13]
									.trim()));
							sd.setPlayingwithChildren(Long.parseLong(fields[14]
									.trim()));
							sd.setJobSearching(Long.parseLong(fields[15].trim()));
							sd.setShopping(Long.parseLong(fields[16].trim()));
							sd.setEatingandDrinking(Long.parseLong(fields[17]
									.trim()));
							sd.setSocializingRelaxing(Long.parseLong(fields[18]
									.trim()));
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

		DataFrame table = sqlContext.applySchema(rdd_records,
				HealthDataModel.class);
		table.registerTempTable("record_table");
		table.printSchema();


		DataFrame res = sqlContext.sql(sqlQuery);
		res.printSchema();
		res.show();
		res.save(outputFile);

	}

}
