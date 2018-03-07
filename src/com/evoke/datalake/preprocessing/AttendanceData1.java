package com.evoke.datalake.preprocessing;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.evoke.datalake.model.AttendanceModel;

import scala.Tuple2;

public class AttendanceData1 {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
		String inputFile = args[0];
		 String inputFileemp = args[1];

		//String inputFile = "C:\\hadoop\\attendence1\\";
		//String inputFileemp = "C:\\hadoop\\employee.csv";
		SparkConf conf = new SparkConf().setAppName("Attendance Calculator").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//SQLContext sqlcon = new HiveContext(sc.sc());
		JavaRDD<String> data = sc.textFile(inputFile);
		JavaRDD<String> empdata = sc.textFile(inputFileemp);
		
		final String header = empdata.collect().get(0);
		
		empdata = empdata.filter(new Function<String, Boolean>() {
			
			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				
				if(!line.equalsIgnoreCase(header))
				{
					return true;
				}
				return false;
			}
		}).filter(new Function<String, Boolean>() {
			
			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				
				String[] temp = line.split(",");
				
				if(!temp[10].trim().equals("Inside Sales") && !temp[10].trim().equals("Evoke"))
				{
					
					if(!temp[9].trim().equals("CSC-Fraud Analysis"))
					{
						//System.out.println(line);
						System.out.println(temp[10]);
						return true;
					}
					else
					{
						
						return false;
					}
					
				}
				return false;
			}
		});
		
		JavaRDD<Long> idrdd = empdata.map(new Function<String, Long>() {

			public Long call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] temp = line.split(",");
				
				return Long.parseLong(temp[1]);
			}
		});
				
		final List<Long> idlist =idrdd.collect();
		
		System.out.println("correct emp list = "+idlist.size());
		
		//System.out.println("emp after filter = "+empdata.count());
		
		//empdata = empdata.
		
		System.out.println(data.count());
		;
		JavaRDD<String[]> temp_rdd = data.map(new Function<String, String[]>() {

			public String[] call(String line) throws Exception {
				// TODO Auto-generated method stub

				return line.split(",");
			}
		});
		System.out.println(temp_rdd.collect().get(0).length);
		
		temp_rdd = temp_rdd.filter(new Function<String[], Boolean>() {
			
			public Boolean call(String[] line) throws Exception {
				// TODO Auto-generated method stub
				long empId = Long.parseLong(line[3]);
				//System.out.println(empId);
				if(idlist.contains(empId))	
				{
					
					//System.out.println("true -------- "+line[3]);
					return true;
				}
				//System.out.println("false -------- "+line[3]);
				return false;
			}
		});

		JavaPairRDD<String, String[]> pair_rdd = temp_rdd.mapToPair(new PairFunction<String[], String, String[]>() {

			public Tuple2<String, String[]> call(String[] line) throws Exception {
				// TODO Auto-generated method stub
				// System.out.println(line[1]);
				
				String dateTime = line[4];
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				Date date = sdf.parse(dateTime);
				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
				// System.out.println(sdf1.format(date)+"_"+line[3]);
				Tuple2<String, String[]> keyvalue = new Tuple2<String, String[]>(sdf1.format(date) + "_" + line[3],
						line);

				return keyvalue;
			}
		});

		JavaPairRDD<String, Iterable<String[]>> group_pair = pair_rdd.groupByKey();
		
		
		
		
		

		JavaRDD<AttendanceModel> temp1_rdd = group_pair.map(new Function<Tuple2<String, Iterable<String[]>>, AttendanceModel>() {

			public AttendanceModel call(Tuple2<String, Iterable<String[]>> tup) throws Exception {
				// TODO Auto-generated method stub

				//System.out.println("tup=" + tup._1);
				TreeMap<Long, String[]> sortMap = new TreeMap<Long, String[]>();
				TreeMap<Long,String[]> inmap = new TreeMap<Long, String[]>();
				TreeMap<Long,String[]> outmap = new TreeMap<Long, String[]>();

				Iterator<String[]> it = tup._2.iterator();
				
				

				while (it.hasNext()) {
					
					String[] temp = it.next();
					
					
					if(temp[5].equalsIgnoreCase("in"))
					{
						inmap.put(Long.parseLong(temp[0]), temp);
					}
					else
					{
						outmap.put(Long.parseLong(temp[0]), temp);
					}
					
					

				}
				
				if(inmap.size()!=0 && outmap.size()!=0)
				{
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					
					
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
					
					//System.out.println("in list"+inmap.size());
					//System.out.println("out list"+outmap.size());
					Date indate = sdf.parse(inmap.firstEntry().getValue()[4]);
					Date outdate = sdf.parse(outmap.lastEntry().getValue()[4]);
					
					long duration  = outdate.getTime() - indate.getTime();
					
					
					long diffInMinutes = TimeUnit.MILLISECONDS.toMinutes(duration);
					long diffInHours = TimeUnit.MILLISECONDS.toHours(duration);
					
					//String timediff = diffInHours+":"+diffInMinutes;
					
					//System.out.println("first = "+sortMap.firstEntry().getValue()[4]);
					//System.out.println("last = "+sortMap.lastEntry().getValue()[4]);
					
					//String returnValue = tup._1.split("_")[0]+","+tup._1.split("_")[1]+","+sdf.format(indate)+","+sdf.format(outdate)+","+diffInHours;
					AttendanceModel temp = new AttendanceModel();
					temp.setEntrydate(tup._1.split("_")[0]);
					temp.setEmpId(Long.parseLong(tup._1.split("_")[1]));
					temp.setIndate(sdf.format(indate));
					temp.setOutdate(sdf.format(outdate));
					System.out.println((double)diffInMinutes/60);
					temp.setDuration(diffInMinutes);
							
					System.out.println(temp.getEntrydate()+","+temp.getEmpId()+","+temp.getDuration());
					return temp;
				}
				
				return null;
				
				
			}
		}).filter(new Function<AttendanceModel, Boolean>() {
			
			public Boolean call(AttendanceModel am) throws Exception {
				// TODO Auto-generated method stub
				
				if(am != null)
				{
					return true;
				}
				return false;
			}
		});
		SQLContext sqlcon = new HiveContext(sc.sc());
		DataFrame df = sqlcon.createDataFrame(temp1_rdd, AttendanceModel.class);

		df.registerTempTable("agg_temp");

		df.printSchema();
		System.out.println("sql count = " + df.count());
		String sqlQuery = "INSERT INTO TABLE attendance_agg  select entrydate,empId,indate,outdate,duration from agg_temp";

		sqlcon.sql(sqlQuery);
		temp1_rdd.count();
	}

}
