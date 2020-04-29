package OnTime.Model;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class model_main {

	// Schema definition
	private final static ImmutableMap<String, DataType> schemaMap = ImmutableMap.<String, DataType>builder()
			.put("departureAirportFsCode_v1", DataTypes.StringType).put("arrivalAirportFsCode_v1", DataTypes.StringType)
			.put("year", DataTypes.StringType).put("month", DataTypes.StringType).put("day", DataTypes.StringType)
			.put("carrierFsCode_v1", DataTypes.StringType).put("flightNumber", DataTypes.StringType)
			.put("fsCodeFlightNumber", DataTypes.StringType).put("departureDate", DataTypes.StringType)
			.put("departuredateUtc", DataTypes.StringType).put("arrivalDate", DataTypes.StringType)
			.put("arrivalDateUtc", DataTypes.StringType).put("flightType", DataTypes.StringType)
			.put("departureGateDelayMinutes", DataTypes.IntegerType).put("temp_v1", DataTypes.FloatType)
			.put("pressure", DataTypes.FloatType).put("weather_v1", DataTypes.StringType)
			.put("wind_speed", DataTypes.FloatType).put("windDeg_v1", DataTypes.FloatType)
			.put("humidity", DataTypes.FloatType).put("ontime", DataTypes.IntegerType)
			.put("late15", DataTypes.IntegerType).put("late30", DataTypes.IntegerType)
			.put("late45", DataTypes.IntegerType).put("delayMean_v1", DataTypes.FloatType)
			.put("delayStandardDeviation", DataTypes.FloatType).put("delayMin", DataTypes.FloatType)
			.put("delayMax", DataTypes.FloatType).put("allOntimeCumulative_v1", DataTypes.FloatType)
			.put("allOntimeStars_v1", DataTypes.FloatType).put("allDelayCumulative_v1", DataTypes.FloatType)
			.put("allDelayStars_v1", DataTypes.FloatType).build();

	/**
	 * Read File
	 * 
	 * @param ss   Spark Session
	 * @param Path path of csv
	 * @return Dataset
	 */
	public static Dataset<Row> readFile(SparkSession ss, String Path) {

		List<StructField> fields = Lists.newArrayList();
		for (String columna : schemaMap.keySet()) {
			fields.add(DataTypes.createStructField(columna, schemaMap.get(columna), false));
		}
		;

		// StructType define la estructura completa de la tabla, con nombres de
		// variables y datatype
		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> file = ss.read().format("csv").option("header", true).option("delimiter", ";")
				.option("nullValue", "null").schema(schema).load(Path);

		return file;
	}

	public static void run(SparkSession ss, String weather, String data_path, String model_path, String Model,
			String multiclass) throws ParseException, IOException {

		// load data
		Dataset<Row> File = readFile(ss, data_path);

		// Preprocessing
		long t = System.currentTimeMillis();
		File = processing.preprocessing(ss, File, weather, Model, multiclass, model_path);
		long t1 = System.currentTimeMillis();

		// Training model
		training.trainingModel(ss, File, Model, model_path, weather, multiclass);
		long t2 = System.currentTimeMillis();

		System.out.println("Fin Modelo OK, Time: " + (t2 - t) / 1000 + " seg");
		System.out.println("Time preprocesing =" + (t1 - t) / 1000 + " seg");
		System.out.println("Time training =" + (t2 - t1) / 1000 + " seg");

		
		
	}

}
