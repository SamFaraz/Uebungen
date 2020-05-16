package sparkdemo;

import java.util.Arrays;
import java.util.List;

//import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class LocalDFQuery {

	public static void main(String[] args) {

	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("JavaWordCount")
	    	      .master("local")
	    	      .getOrCreate();
	    
	    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
	    Dataset<Integer> distDF = spark.createDataset(data, Encoders.INT());
	    System.out.println(distDF.schema());
	    distDF.show();
	    Dataset<Integer> dfEven = distDF.filter("value > 3");
	    Dataset<Integer> dfEven2 = distDF.filter(distDF.col("value").gt(Integer.valueOf(3)));
	    dfEven2.show();
	}

}
