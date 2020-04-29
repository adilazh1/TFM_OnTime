package OnTime.routeStatusSpark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Utils {

	public static JavaSparkContext ContextStart(String DBdatabase,String DBcollection) {
		
		String database_collection = DBdatabase+"."+DBcollection;
		SparkSession spark = SparkSession.builder().master("local[2]").appName("routeStatusSpark")
				.config("spark.mongodb.input.uri", "mongodb://localhost:27017/"+database_collection)
				.config("spark.mongodb.output.uri", "mongodb://localhost:27017/"+database_collection)
				.config("spark.mongodb.output.maxBatchSize", "1024").getOrCreate();

		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		
		return context;
	}
}
