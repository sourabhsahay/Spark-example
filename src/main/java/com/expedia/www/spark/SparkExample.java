package com.expedia.www.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkExample {

	// Provide the program arguments as follows :   -dirTillDate/**/* 
	// This would simulate the repository that we have in AWS for UIS messages
	//An example, userinteractionmessages-test/raw/test/2015/01/01/00 is a path which is of format
	//year/month/day and then hours within each day.
	
	
	public static void main(String[] args) {
		String logFile = args[0];
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		

		JavaRDD<String> allGuids = logData.map(
				new Function<String, String>() {
					public String call(String line) throws Exception {
						ObjectMapper mapper = new ObjectMapper();
						try {
							JsonNode actualObj = mapper.readTree(line);
							JsonNode pageInfoObject = actualObj.get("pageInfo");
							if(pageInfoObject.get("pageName")!=null && pageInfoObject.get("pageName").asText().contains("Confirmation"))
							{
								System.out.println(pageInfoObject.get("pageName"));
								JsonNode contextObject = actualObj.get("context");
								if(contextObject.get("user").get("guid")!=null)
								{
									System.out.println(contextObject.get("user").get("guid"));
									return contextObject.get("user").get("guid").textValue();
								}
								else
								{
									System.out.println("User User Info found");
									return "";
								}
							}
							
							else return "";
							

						} catch (JsonProcessingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return "";
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return "";
						}
					}
				});
		
		JavaRDD<String> bookingUISMessages = allGuids.filter(new Function<String, Boolean>() {
	        public Boolean call(String s) {
	        	return !"".equals(s);
	        }
	      });
		
		bookingUISMessages.saveAsTextFile("abc.txt");

	}

}
