package sparkdemo;

import java.util.Arrays;
import java.util.List;

//import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class LocalRDDMap {

	public static void main(String[] args) {

	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("JavaWordCount")
	    	      .master("local")
	    	      .getOrCreate();
	    
	    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5); 
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
	    JavaRDD<Integer> distData = sc.parallelize(data);
	    JavaRDD<Integer> d2 = distData.map(x -> x * 2);
	    List<Integer> res = d2.collect();
	    System.out.println(res);

	}

}
