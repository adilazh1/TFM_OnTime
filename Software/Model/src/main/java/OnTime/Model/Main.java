package OnTime.Model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import scala.collection.immutable.Map;

public class Main {
	static String HADOOP_COMMON_PATH = "C:\\Users\\adilazh1\\Documents\\Universidad\\Master\\Laboratorio\\Spark\\Mllib\\sesion_01\\Spam_Detection\\Spark_MLLib_I_II\\src\\main\\resources\\winutils";

	public static void main(String[] args) throws ParseException, IOException {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

		// Logger OFF for spark, mongoDB etc
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Auxiliary variables
		String data_path = "";
		String model_path = "";
		String weather = "";
		String model = "";
		String multiclass = "";

		// read properties file
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));

			model_path = properties.getProperty("model_path");
			data_path = properties.getProperty("data_path");
			weather = properties.getProperty("weather");
			model = properties.getProperty("model");
			multiclass = properties.getProperty("multiclass");

		} catch (FileNotFoundException e) {
			System.out.println("Error, propeties file don't exist");
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println("Error, can't read properties file");
			System.out.println(e.getMessage());
		}

		// Create a Spark conector

		SparkSession spark = SparkSession.builder().master("local[*]").appName("Model")
				.config("spark.rdd.compress", true).config("spark.executor.memory", "3G")
				.config("spark.driver.memory", "3G").getOrCreate();
		
		// Version Cluster
//		SparkSession spark = SparkSession.builder().master("spark://master:7077").appName("Model").getOrCreate();

		Map<String, String> conf = spark.conf().getAll();
		System.out.println("Spark conf: " + conf);

		model_main.run(spark, weather, data_path, model_path, model, multiclass);

		spark.stop();
	}

}
