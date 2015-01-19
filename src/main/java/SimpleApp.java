import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.SequenceFileRDDFunctions;
import org.json4s.jackson.Json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SimpleApp {
  public static void main(String[] args) {
    //String logFile = "/Users/vsahay/spark-1.1.0-bin-hadoop1/README.md"; // Should be some file on your system
	String logFile = args[0];
    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();
/*    org.apache.hadoop.conf.Configuration hadoopConf = sc.hadoopConfiguration();
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
    hadoopConf.set("fs.s3.awsAccessKeyId","");
    hadoopConf.set("fs.s3.awsSecretAccessKey","");
*/    
    System.out.println(logData);//logData.saveAsTextFile("sample1.json");

    JavaRDD<String> numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("Spark") ; }
    });
    
    
    
    JavaPairRDD<String, String> onesw= logData.mapToPair(
    		  new PairFunction<String, String, String>() {
    		    public Tuple2<String, String> call(String s) {
    		    	ObjectMapper mapper = new ObjectMapper();
    		    	try {
						JsonNode actualObj = mapper.readTree(s);
						JsonNode contextObject = actualObj.get("context");
						System.out.println(contextObject.get("clientInfo").get("ipAddress"));
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    		    System.out.println("i m here");
    		    System.out.println("i m here");
    		      return new Tuple2(s, "1");
    		    }
    		  }
    		);
    
    JavaPairRDD<String, String> counts = onesw.reduceByKey(new Function2<String, String, String>()
    		{

				public String call(String v1, String v2) throws Exception {
					System.out.println(v1);
					System.out.println(v2);
					return  v1 + " : " +  v2;
				}	
    	
    		});
    
    
	counts.foreach(new VoidFunction<Tuple2<String, String>>()
			{

				public void call(Tuple2<String, String> t) throws Exception {
					PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("myfile.txt", true)));
					out.println(t._1());
					out.close();
				}
		
			});
    
    
    List<Tuple2<String, String>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    
    

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();
    
    
    
   /* 
    JavaPairRDD<String, String> ones =numAs.mapToPair(
  		  new PairFunction<String, String, String>() {
  		    public Tuple2<String, String> call(String s) {
  		      if(s.startsWith("Spark"))
  		      {
  		    	return new Tuple2("Spark",s);
  		      }
  		      else if(s.startsWith("b"))
  		      {
  		      return new Tuple2("b", s);
  		      }
  		      else return new Tuple2("", "");
  		    }
  		  }
  		);
    JavaPairRDD<String, String> counts = ones.reduceByKey(new Function2<String, String, String>()
    		{

				public String call(String v1, String v2) throws Exception {
					System.out.println(v1);
					System.out.println(v2);
					return  v1 + " : " +  v2;
				}	
    	
    		});
    
    List<Tuple2<String, String>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }*/


  }
}