package org.test.spark.app;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkApp {

	private static Logger LOGGER = LoggerFactory.getLogger(SparkApp.class);
	private static SQLContext sqlContext = null;
	
	public static void main(String[] args) {
		if(args.length != 1){
			System.out.println("Terminating...");
			System.exit(0);
		}
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("poor-ratio-decider");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		sqlContext = new SQLContext(jsc);
		
		JavaRDD<String> inputData = jsc.textFile(args[0]);
		JavaRDD<DataPoint> dataPointRDD = inputData.map(new Function<String, DataPoint>() {
			private static final long serialVersionUID = 1L;

			public DataPoint call(String record) throws Exception {
				String[] contents = record.split(",");
				DataPoint dp = new DataPoint();
				dp.setIp(contents[0]);
				dp.setDevice_type(contents[1]);
				dp.setScore(Float.parseFloat(contents[2]));
				return dp;
			}
		});
		
		DataFrame urlsDF = sqlContext.createDataFrame(dataPointRDD, DataPoint.class);
		urlsDF.registerTempTable("datapoint");
		
		DataFrame scoreCalculator = sqlContext.sql("select ip, device_type, if(avg(score)<=50, \"POOR\", \"RICH\") as result from datapoint group by ip, device_type");
		scoreCalculator.registerTempTable("poorresults");
		DataFrame poorResult = sqlContext.sql("select device_type,count(*) from poorresults where result=\"POOR\" group by device_type");
		DataFrame totalResult = sqlContext.sql("select device_type,count(*) from poorresults group by device_type");
		List<String> poorrResultDisp = poorResult.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Row row) {
			    return "" + row.getString(0) + "," + row.getLong(1);
			  }
			}).collect();
		
		Map<String, Integer> poorResultMap = new HashMap<String, Integer>();
		
		for(String s: poorrResultDisp){
			String[] poorResultArr= s.split(",");
			poorResultMap.put(poorResultArr[0], Integer.parseInt(poorResultArr[1]));
		}
		
		List<String> totalResultDisp = totalResult.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Row row) {
			    return "" + row.getString(0) + "," + row.getLong(1);
			  }
			}).collect();
		
		Map<String, Integer> totalResultMap = new HashMap<String, Integer>();
		for(String s: totalResultDisp){
			String[] totalResultArr= s.split(",");
			totalResultMap.put(totalResultArr[0], Integer.parseInt(totalResultArr[1]));
		}
		
		Map<String, Float> finalResultMap = new HashMap<String, Float>();
		
		for (Map.Entry<String, Integer> entry : poorResultMap.entrySet()){
			finalResultMap.put(entry.getKey(), (float)entry.getValue()/totalResultMap.get(entry.getKey()));
		}

		finalResultMap.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).forEach(System.out::println);
	}

}
