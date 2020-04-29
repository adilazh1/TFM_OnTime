package com.fly.ontime.predict;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.sql.catalyst.expressions.Concat;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;
import org.spark_project.guava.base.Functions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientDelegate;
import com.mongodb.client.model.Filters;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class Predict {
	// Schema definition V2 : the schema have to be = document in variables order
	private final static ImmutableMap<String, DataType> schemaMap_v2 = ImmutableMap.<String, DataType>builder()
			/*
			 * General Data
			 */
			// Input user information
			.put("departureAirportFsCode", DataTypes.StringType).put("arrivalAirportFsCode", DataTypes.StringType)
			.put("departureDate", DataTypes.StringType).put("wPrecio", DataTypes.IntegerType)
			.put("wRetardo", DataTypes.IntegerType).put("wDuracion", DataTypes.IntegerType)
			// idConectioin+direct
			.put("idConnection", DataTypes.StringType).put("directo", DataTypes.BooleanType)
			// Flight v1 information
			.put("departureAirportFsCode_v1", DataTypes.StringType).put("fSalida_v1", DataTypes.StringType)
			.put("hSalida_v1", DataTypes.StringType).put("carrierFsCode_v1", DataTypes.StringType)
			.put("flightNumber_v1", DataTypes.StringType).put("arrivalAirportFsCode_v1", DataTypes.StringType)
			.put("fLlegada_v1", DataTypes.StringType).put("hLlegada_v1", DataTypes.StringType)
			.put("codeshares_v1", DataTypes.StringType).put("temp_v1", DataTypes.DoubleType)
			.put("humidity_v1", DataTypes.DoubleType).put("pressure_v1", DataTypes.DoubleType)
			.put("weather_v1", DataTypes.StringType).put("windSpeed_v1", DataTypes.DoubleType)
			.put("windDeg_v1", DataTypes.DoubleType).put("ontime_v1", DataTypes.IntegerType)
			.put("late15_v1", DataTypes.IntegerType).put("late30_v1", DataTypes.IntegerType)
			.put("late45_v1", DataTypes.IntegerType).put("delayMean_v1", DataTypes.FloatType)
			.put("delayStandardDeviation_v1", DataTypes.FloatType).put("delayMin_v1", DataTypes.FloatType)
			.put("delayMax_v1", DataTypes.FloatType).put("allOntimeCumulative_v1", DataTypes.FloatType)
			.put("allOntimeStars_v1", DataTypes.FloatType).put("allDelayCumulative_v1", DataTypes.FloatType)
			.put("allDelayStars_v1", DataTypes.FloatType)
			// Flight v2 information
			.put("departureAirportFsCode_v2", DataTypes.StringType).put("fSalida_v2", DataTypes.StringType)
			.put("hSalida_v2", DataTypes.StringType).put("carrierFsCode_v2", DataTypes.StringType)
			.put("flightNumber_v2", DataTypes.StringType).put("arrivalAirportFsCode_v2", DataTypes.StringType)
			.put("fLlegada_v2", DataTypes.StringType).put("hLlegada_v2", DataTypes.StringType)
			.put("codeshares_v2", DataTypes.StringType).put("temp_v2", DataTypes.DoubleType)
			.put("humidity_v2", DataTypes.DoubleType).put("pressure_v2", DataTypes.DoubleType)
			.put("weather_v2", DataTypes.StringType).put("windSpeed_v2", DataTypes.DoubleType)
			.put("windDeg_v2", DataTypes.DoubleType).put("ontime_v2", DataTypes.IntegerType)
			.put("late15_v2", DataTypes.IntegerType).put("late30_v2", DataTypes.IntegerType)
			.put("late45_v2", DataTypes.IntegerType).put("delayMean_v2", DataTypes.FloatType)
			.put("delayStandardDeviation_v2", DataTypes.FloatType).put("delayMin_v2", DataTypes.FloatType)
			.put("delayMax_v2", DataTypes.FloatType).put("allOntimeCumulative_v2", DataTypes.FloatType)
			.put("allOntimeStars_v2", DataTypes.FloatType).put("allDelayCumulative_v2", DataTypes.FloatType)
			.put("allDelayStars_v2", DataTypes.FloatType)
			// price information
			.put("flightNumber_v1_py", DataTypes.StringType).put("flightNumber_v2_py", DataTypes.StringType)
			.put("precio", DataTypes.StringType).put("duracion", DataTypes.StringType)
			.put("enlaces", DataTypes.StringType).build();

	/**
	 * Read input file
	 * 
	 * @param ss
	 * @param Path
	 * @return dataset
	 */
	public static Dataset<Row> readFile(SparkSession ss, String Path) {

		List<StructField> fields = Lists.newArrayList();
		for (String columna : schemaMap_v2.keySet()) {
			fields.add(DataTypes.createStructField(columna, schemaMap_v2.get(columna), false));
		}
		;

		StructType schema = DataTypes.createStructType(fields);

		
		Dataset<Row> file = ss.read().format("csv").option("header", true).option("delimiter", ";")
				.option("nullValue", "null").schema(schema).load(Path);

		return file;
	}

	/**
	 * Pre-procession Categorize continuous variables like dates, statistiques of
	 * flights etc
	 * 
	 * @param ss
	 * @param in
	 * @param withWeather
	 * @return Dataset
	 */
	public static Dataset<Row> preprocessing(SparkSession ss, Dataset<Row> in, boolean withWeather) {
		// Categorize month
		ss.udf().register("getMonth", new UDF1<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(final String departureDate) {

				HashMap<String, String> months = new HashMap<String, String>();
				months.put("01", "Jan");
				months.put("02", "Feb");
				months.put("03", "Mar");
				months.put("04", "Apr");
				months.put("05", "May");
				months.put("06", "Jun");
				months.put("07", "Jul");
				months.put("08", "Aug");
				months.put("09", "Sep");
				months.put("10", "Oct");
				months.put("11", "Mov");
				months.put("12", "Dec");

				String departureMonth = null;
				String month = departureDate.split("-")[1];
				departureMonth = months.get(month);

				return departureMonth;
			}
		}, DataTypes.StringType);

		// Categorize day
		ss.udf().register("getDay", new UDF1<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(final String departureDate) throws ParseException {

				String departureDay = null;
				departureDay = getDay(departureDate);
				return departureDay;
			}
		}, DataTypes.StringType);

		// Categorize moment of day. Day is partitioned into 24/3 categories
		ss.udf().register("getDayMoment", new UDF1<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(final String hSalida) throws ParseException {

				String dayMoment = null;

				Integer h = Integer.valueOf(hSalida.split(":")[0]);
				Integer m = Integer.valueOf(hSalida.split(":")[1]);
				Integer M = (h * 60) + m;
				Integer mod = M / 180;

				dayMoment = "d" + mod;

				return dayMoment;
			}
		}, DataTypes.StringType);

		in = in.withColumn("departureMonth_v1", functions.callUDF("getMonth", in.col("fSalida_v1")));
		in = in.withColumn("arrivalMonth_v1", functions.callUDF("getMonth", in.col("fLlegada_v1")));
		in = in.withColumn("departureDay_v1", functions.callUDF("getDay", in.col("fSalida_v1")));
		in = in.withColumn("arrivalDay_v1", functions.callUDF("getDay", in.col("fLlegada_v1")));
		in = in.withColumn("departureDayMoment_v1", functions.callUDF("getDayMoment", in.col("hSalida_v1")));
		in = in.withColumn("arrivalDayMoment_v1", functions.callUDF("getDayMoment", in.col("hLlegada_v1")));

		// categorizeDelayAndWeather

		double[] splits = new double[] { 0.0, 15.0, Double.MAX_VALUE };
		Bucketizer delayMean_v1 = new Bucketizer().setInputCol("delayMean_v1").setOutputCol("delayMeanLabel_v1")
				.setSplits(splits);

		// Categorize Temperature. Giving in Kelving
		double[] split1 = { 0.0, 268.0, 308.0, Double.MAX_VALUE };
		Bucketizer temp_v1 = new Bucketizer().setInputCol("temp_v1").setOutputCol("tempLabel_v1").setSplits(split1);

		// Categorize wind deg in 18 categories
		double[] split2 = { 0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 220.0, 240.0, 260.0,
				280.0, 300.0, 320.0, 340.0, Double.MAX_VALUE };
		Bucketizer windDeg_v1 = new Bucketizer().setInputCol("windDeg_v1").setOutputCol("windDegLabel_v1")
				.setSplits(split2);


		// First, we prepare the latest transformation before indexing categorical
		// variables
		if (withWeather) {

			Pipeline pipeline = new Pipeline().setStages(
					new PipelineStage[] { delayMean_v1, temp_v1, windDeg_v1 });

			PipelineModel model = pipeline.fit(in);
			in = model.transform(in);

		} else {

			Pipeline pipeline = new Pipeline()
					.setStages(new PipelineStage[] { delayMean_v1 });

			PipelineModel model = pipeline.fit(in);
			in = model.transform(in);
		}

		return in;
	}

	/**
	 * Load StringIndexerModel and transform new data. Method include OneHoteEnvoder
	 * 
	 * @param data
	 * @param withWeather
	 * @param pipe        Pipeline
	 * @return Dataset
	 */
	public static Dataset<Row> indexer(Dataset<Row> data, boolean withWeather, Pipeline pipe) {

		if (withWeather) {

			// load StringIndexerModel

			data = pipe.fit(data).transform(data);

			// OneHot
			OneHotEncoderEstimator oneHot = new OneHotEncoderEstimator()
					.setInputCols(new String[] { "departureAirportIndx_v1", "arrivalAirportIndx_v1",
							"carrierFsCodeIndx_v1", "weatherIndx_v1", "tempIndx_v1", "windDegIndx_v1",
							"departureMonthIndx_v1", "arrivalMonthIndx_v1", "departureDayIndx_v1", "arrivalDayIndx_v1",
							"departureDayMomentIndx_v1", "arrivalDayMomentIndx_v1", "delayMeanLabelIndx_v1" })
					.setOutputCols(new String[] { "departureAirportOneHot_v1", "arrivalAirportOneHot_v1",
							"carrierFsCodeOneHot_v1", "weatherOneHot_v1", "tempOneHot_v1", "windDegOneHot_v1",
							"departureMonthOneHot_v1", "arrivalMonthOneHot_v1", "departureDayOneHot_v1",
							"arrivalDayOneHot_v1", "departureDayMomentOneHot_v1", "arrivalDayMomentOneHot_v1",
							"delayMeanLabelOneHot_v1" });

			data = oneHot.fit(data).transform(data);
			// data.show(5);
		} else {

			data = pipe.fit(data).transform(data);

			// OneHot
			OneHotEncoderEstimator oneHot = new OneHotEncoderEstimator()
					.setInputCols(
							new String[] { "departureAirportIndx_v1", "arrivalAirportIndx_v1", "carrierFsCodeIndx_v1",
									"departureMonthIndx_v1", "arrivalMonthIndx_v1", "departureDayIndx_v1",
									"arrivalDayIndx_v1", "departureDayMomentIndx_v1", "arrivalDayMomentIndx_v1",
									"delayMeanLabelIndx_v1" })
					.setOutputCols(new String[] { "departureAirportOneHot_v1", "arrivalAirportOneHot_v1",
							"carrierFsCodeOneHot_v1", "departureMonthOneHot_v1", "arrivalMonthOneHot_v1",
							"departureDayOneHot_v1", "arrivalDayOneHot_v1", "departureDayMomentOneHot_v1",
							"arrivalDayMomentOneHot_v1", "delayMeanLabelOneHot_v1"});

			data = oneHot.fit(data).transform(data);

		}
		return data;
	}

	/**
	 * Feature selection,
	 * 
	 * @param data
	 * @param withWeather Y or N
	 * @return
	 */
	public static Dataset<Row> featureSelection(Dataset<Row> data, boolean withWeather) {

		if (withWeather) {

			String[] features = { "departureAirportOneHot_v1", "arrivalAirportOneHot_v1", "carrierFsCodeOneHot_v1",
					"tempOneHot_v1", "humidity_v1", "pressure_v1", "weatherOneHot_v1", "windSpeed_v1",
					"windDegOneHot_v1", "departureMonthOneHot_v1", "arrivalMonthOneHot_v1", "departureDayOneHot_v1",
					"arrivalDayOneHot_v1", "departureDayMomentOneHot_v1", "arrivalDayMomentOneHot_v1",
					"delayMeanLabelOneHot_v1", "allOntimeCumulative_v1",
					"allDelayCumulative_v1" };

			VectorAssembler assembler_v1 = new VectorAssembler().setInputCols(features).setOutputCol("features");
			data = assembler_v1.transform(data);

			// System.out.println("With weather, number of variables=" + features.length);
		} else {

			String[] features = { "departureAirportOneHot_v1", "arrivalAirportOneHot_v1", "carrierFsCodeOneHot_v1",
					"departureMonthOneHot_v1", "arrivalMonthOneHot_v1", "departureDayOneHot_v1", "arrivalDayOneHot_v1",
					"departureDayMomentOneHot_v1", "arrivalDayMomentOneHot_v1", "delayMeanLabelOneHot_v1",
					"allOntimeCumulative_v1", "allDelayCumulative_v1" };

			VectorAssembler assembler_v1 = new VectorAssembler().setInputCols(features).setOutputCol("features");
			data = assembler_v1.transform(data);

			// System.out.println("Without weather, number of variables=" +
			// features.length);
		}

		return data;

	}

	/**
	 * Return the day from a giving date
	 * 
	 * @param Date format yyyy-mm-dd
	 * @return day of week's name
	 * @throws ParseException
	 */
	public static String getDay(String Date) throws ParseException {

		String input_date = Date;
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
		Date dt1 = format1.parse(input_date);
		DateFormat format2 = new SimpleDateFormat("EEE", Locale.ENGLISH);
		String finalDay = format2.format(dt1);
		return finalDay;

	}

	/**
	 * Method to load model
	 * 
	 * @param ss         SparkSesion
	 * @param in         dataset
	 * @param model      model to use: RandomForest,... see model.properties file
	 * @param model_name
	 * @return Dataset with predictions
	 */
	public static Dataset<Row> loadModel(SparkSession ss, Dataset<Row> in, Object model, String model_name) {

		// System.out.println("load MODEL");

		switch (model_name) {
		case "RF":
			in = ((RandomForestClassificationModel) model).transform(in);
			break;
		case "RF_CV":
			in = ((CrossValidatorModel) model).transform(in);
			break;
		case "GBTC":
			in = ((GBTClassificationModel) model).transform(in);
			break;
		case "LinearSVM":
			in = ((CrossValidatorModel) model).transform(in);
			break;
		case "OneVsRestLR":
			in = ((OneVsRestModel) model).transform(in);
			break;
		case "OneVsRestSVM":
			in = ((OneVsRestModel) model).transform(in);
			break;
		case "LR":
			in = ((CrossValidatorModel) model).transform(in);
			break;
		case "NB":
			in = ((NaiveBayesModel) model).transform(in);
			break;
		default:

			break;

		}

		return in;

	}

	/**
	 * Change names V1-->V0 V2-->V1
	 * 
	 * @return
	 */
	public static Dataset<Row> changeNames(Dataset<Row> dataset) {

		dataset = dataset
				// V1-->V0
				.withColumnRenamed("departureAirportFsCode_v1", "departureAirportFsCode_v0")
				.withColumnRenamed("fSalida_v1", "fSalida_v0").withColumnRenamed("hSalida_v1", "hSalida_v0")
				.withColumnRenamed("carrierFsCode_v1", "carrierFsCode_v0")
				.withColumnRenamed("flightNumber_v1", "flightNumber_v0")
				.withColumnRenamed("arrivalAirportFsCode_v1", "arrivalAirportFsCode_v0")
				.withColumnRenamed("fLlegada_v1", "fLlegada_v0").withColumnRenamed("hLlegada_v1", "hLlegada_v0")
				.withColumnRenamed("codeshares_v1", "codeshares_v0").withColumnRenamed("temp_v1", "temp_v0")
				.withColumnRenamed("humidity_v1", "humidity_v0").withColumnRenamed("pressure_v1", "pressure_v0")
				.withColumnRenamed("weather_v1", "weather_v0").withColumnRenamed("windSpeed_v1", "windSpeed_v0")
				.withColumnRenamed("windDeg_v1", "windDeg_v0").withColumnRenamed("ontime_v1", "ontime_v0")
				.withColumnRenamed("late15_v1", "late15_v0").withColumnRenamed("late30_v1", "late30_v0")
				.withColumnRenamed("late45_v1", "late45_v0").withColumnRenamed("delayMean_v1", "delayMean_v0")
				.withColumnRenamed("delayStandardDeviation_v1", "delayStandardDeviation_v0")
				.withColumnRenamed("delayMin_v1", "delayMin_v0").withColumnRenamed("delayMax_v1", "delayMax_v0")
				.withColumnRenamed("allOntimeCumulative_v1", "allOntimeCumulative_v0")
				.withColumnRenamed("allOntimeStars_v1", "allOntimeStars_v0")
				.withColumnRenamed("allDelayCumulative_v1", "allDelayCumulative_v0")
				.withColumnRenamed("allDelayStars_v1", "allDelayStars_v0")
				.withColumnRenamed("departureMonth_v1", "departureMonth_v0")
				.withColumnRenamed("arrivalMonth_v1", "arrivalMonth_v0")
				.withColumnRenamed("departureDay_v1", "departureDay_v0")
				.withColumnRenamed("arrivalDay_v1", "arrivalDay_v0")
				.withColumnRenamed("flightNumber_v1_py", "flightNumber_v0_py")
				// V2-->V1
				.withColumnRenamed("departureAirportFsCode_v2", "departureAirportFsCode_v1")
				.withColumnRenamed("fSalida_v2", "fSalida_v1").withColumnRenamed("hSalida_v2", "hSalida_v1")
				.withColumnRenamed("carrierFsCode_v2", "carrierFsCode_v1")
				.withColumnRenamed("flightNumber_v2", "flightNumber_v1")
				.withColumnRenamed("arrivalAirportFsCode_v2", "arrivalAirportFsCode_v1")
				.withColumnRenamed("fLlegada_v2", "fLlegada_v1").withColumnRenamed("hLlegada_v2", "hLlegada_v1")
				.withColumnRenamed("codeshares_v2", "codeshares_v1").withColumnRenamed("temp_v2", "temp_v1")
				.withColumnRenamed("humidity_v2", "humidity_v1").withColumnRenamed("pressure_v2", "pressure_v1")
				.withColumnRenamed("weather_v2", "weather_v1").withColumnRenamed("windSpeed_v2", "windSpeed_v1")
				.withColumnRenamed("windDeg_v2", "windDeg_v1").withColumnRenamed("ontime_v2", "ontime_v1")
				.withColumnRenamed("late15_v2", "late15_v1").withColumnRenamed("late30_v2", "late30_v1")
				.withColumnRenamed("late45_v2", "late45_v1").withColumnRenamed("delayMean_v2", "delayMean_v1")
				.withColumnRenamed("delayStandardDeviation_v2", "delayStandardDeviation_v1")
				.withColumnRenamed("delayMin_v2", "delayMin_v1").withColumnRenamed("delayMax_v2", "delayMax_v1")
				.withColumnRenamed("allOntimeCumulative_v2", "allOntimeCumulative_v1")
				.withColumnRenamed("allOntimeStars_v2", "allOntimeStars_v1")
				.withColumnRenamed("allDelayCumulative_v2", "allDelayCumulative_v1")
				.withColumnRenamed("allDelayStars_v2", "allDelayStars_v1")
				.withColumnRenamed("departureMonth_v2", "departureMonth_v1")
				.withColumnRenamed("arrivalMonth_v2", "arrivalMonth_v1")
				.withColumnRenamed("departureDay_v2", "departureDay_v1")
				.withColumnRenamed("arrivalDay_v2", "arrivalDay_v1")
				.withColumnRenamed("flightNumber_v2_py", "flightNumber_v1_py")
				// predict name

				.withColumnRenamed("rawPrediction", "rawPrediction_v0")
				// .withColumnRenamed("probability", "probability_v0")
				.withColumnRenamed("prediction", "prediction_v0");

		return dataset;

	}

	/**
	 * Drop temporal variables
	 * 
	 * @param dataset
	 * @return
	 */
	public static Dataset<Row> drop(Dataset<Row> dataset) {

		dataset = dataset.drop("departureDayMoment_v1", "arrivalDayMoment_v1", "delayMeanLabel_v1", "tempLabel_v1",
				"windDegLabel_v1", "departureAirportIndx_v1",
				"arrivalAirportIndx_v1", "carrierFsCodeIndx_v1", "tempIndx_v1", "weatherIndx_v1", "windDegIndx_v1",
				"departureMonthIndx_v1", "arrivalMonthIndx_v1", "departureDayIndx_v1", "arrivalDayIndx_v1",
				"departureDayMomentIndx_v1", "arrivalDayMomentIndx_v1", "delayMeanLabelIndx_v1",
				  "weatherOneHot_v1", "carrierFsCodeOneHot_v1",
				"departureAirportOneHot_v1", "arrivalMonthOneHot_v1", "departureMonthOneHot_v1", "tempOneHot_v1",
				"arrivalAirportOneHot_v1", "departureDayOneHot_v1",  "arrivalDayOneHot_v1",
				 "departureDayMomentOneHot_v1", "arrivalDayMomentOneHot_v1",
				"windDegOneHot_v1", "delayMeanLabelOneHot_v1", "features", "probability");

		return dataset;
	}

	/**
	 * Pre-processing: Duration in minutes and prediction approach of minutes also,
	 * but the model used is a binary classification, its possible use multicalss
	 * classifications, but with free data our model not give a good accuracy.
	 * 
	 * @param ss
	 * @param in
	 * @return
	 */
	public static Dataset<Row> processiong4S(SparkSession ss, Dataset<Row> in) {

		ss.udf().register("getDuration", new UDF1<String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(final String duracion) throws ParseException {

				Integer durationMin = null;

				Integer h = Integer.valueOf(duracion.split(" ")[0].replace("h", ""));
				Integer m = Integer.valueOf(duracion.split(" ")[1].replace("m", ""));
				durationMin = (h * 60) + m;

				return durationMin;
			}
		}, DataTypes.IntegerType);

		ss.udf().register("getPrediction", new UDF2<Double, Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Double prediction_v0, Double prediction) throws Exception {
				
				Double predicitionTotal = 0.0;
				if (prediction != null) {
					predicitionTotal = prediction_v0 + prediction;
				} else {
					predicitionTotal = prediction_v0;
				}
				return 15.0 * predicitionTotal;
			}
		}, DataTypes.DoubleType);

		in = in.withColumn("duracionMin", functions.callUDF("getDuration", in.col("duracion")));
		in = in.withColumn("predictionTotal",
				functions.callUDF("getPrediction", in.col("prediction_v0"), in.col("prediction")));

		return in;
	}

	/**
	 * Calculate statistics like average, variance of Price, Duration and retard.
	 * Also give a pondered rating of the flights. Column "S"
	 * 
	 * @param ss
	 * @param in
	 * @return
	 */
	public static Dataset<Row> processiong4S_2(SparkSession ss, Dataset<Row> in) {

		Dataset<Row> aa = ss.createDataFrame(in.toJavaRDD(), in.schema());
		aa.createOrReplaceTempView("table");
		aa.cache();
		Dataset<Row> estadisticos = ss.sql("SELECT avg(precio) AS precioMean, STDDEV(precio) AS precioSTD,"
				+ "avg(duracionMin) AS duracionMean, STDDEV(duracionMin) AS duracionSTD,"
				+ "avg(predictionTotal) AS retrasoMean, STDDEV(predictionTotal) AS retrasoSTD  from table");

		Row row = estadisticos.first();
		double precioMean = row.getDouble(0);
		double precioSTD = row.getDouble(1);
		double duracionMean = row.getDouble(2);
		double duracionSTD = row.getDouble(3);
		double retrasoMean = row.getDouble(4);
		double retrasoSTD = row.getDouble(5);

		ss.udf().register("normalize", new UDF3<String, Double, Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(String Xi, Double muXi, Double SigmaXi) throws Exception {
				
				Double XiNormalized = 0.0;
				if (XiNormalized == 0.0)
					SigmaXi = 1.0;
				XiNormalized = (Double.parseDouble(Xi) - muXi) / (SigmaXi);
				return XiNormalized;

			}
		}, DataTypes.DoubleType);

		ss.udf().register("calcularS", new UDF6<Integer, Integer, Integer, Double, Double, Double, Double>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Double call(Integer Wprecio, Integer wRetardo, Integer wDuracion, Double precioNormalized,
					Double retrasoNormalized, Double duracionNormalized) throws Exception {
				
				Double Si = 0.0;
				Si = (Wprecio * precioNormalized + wRetardo * retrasoNormalized + wDuracion * duracionNormalized)
						/ (1 + Wprecio + wRetardo + wDuracion);
				return Si;

			}
		}, DataTypes.DoubleType);

		in = in.withColumn("precioMean", functions.lit(precioMean));
		in = in.withColumn("precioSTD", functions.lit(precioSTD));

		in = in.withColumn("duracionMean", functions.lit(duracionMean));
		in = in.withColumn("duracionSTD", functions.lit(duracionSTD));

		in = in.withColumn("retrasoMean", functions.lit(retrasoMean));
		in = in.withColumn("retrasoSTD", functions.lit(retrasoSTD));

		in = in.withColumn("precioNormalized",
				functions.callUDF("normalize", in.col("precio"), in.col("precioMean"), in.col("precioSTD")));
		in = in.withColumn("duracionNormalized", functions.callUDF("normalize",
				in.col("duracionMin").cast(DataTypes.StringType), in.col("duracionMean"), in.col("duracionSTD")));
		in = in.withColumn("retrasoNormalized", functions.callUDF("normalize",
				in.col("predictionTotal").cast(DataTypes.StringType), in.col("retrasoMean"), in.col("retrasoSTD")));

		in = in.withColumn("S",
				functions.callUDF("calcularS", in.col("wPrecio"), in.col("wRetardo"), in.col("wDuracion"),
						in.col("precioNormalized"), in.col("retrasoNormalized"), in.col("duracionNormalized")));
		return in;
	}

	public static Dataset<Row> fit(String path_csv, SparkSession ss, boolean withWeather, String path_indx,
			String path_model, String model_name) throws AnalysisException {

		// load the csv data set
		Dataset<Row> out = null;

		// load csv
		out = readFile(ss, path_csv);

		// load Indexes and model
		Pipeline pipe = Pipeline.load(path_indx);
		Object model = null;
		switch (model_name) {
		case "RF":
			model = (RandomForestClassificationModel) RandomForestClassificationModel.load(path_model);
			break;
		case "RF_CV":
			model = (CrossValidator) CrossValidator.load(path_model);
			break;
		case "GBTC":
			model = (GBTClassificationModel) GBTClassificationModel.load(path_model);
			break;
		case "LinearSVM":
			model = (CrossValidatorModel) CrossValidatorModel.load(path_model);
			break;
		case "OneVsRestLR":
			model = (OneVsRestModel) OneVsRestModel.load(path_model);
			break;
		case "OneVsRestSVM":
			model = (OneVsRestModel) OneVsRestModel.load(path_model);
			break;
		case "LR":
			model = (CrossValidatorModel) CrossValidatorModel.load(path_model);
			break;
		case "NB":
			model = (NaiveBayesModel) NaiveBayesModel.load(path_model);
			break;
		default:

			break;
		}

		Dataset<Row> directo = out.filter("directo=true");
		Dataset<Row> escala = out.filter("directo=false");

		// Getting Ratings from MongoDB
		long t = System.currentTimeMillis();

		// Pre-procession
		long t1 = System.currentTimeMillis();
		// path_indx depende de si se llama al modelo con datos del clima o no

		// direct flight
		directo = preprocessing(ss, directo, withWeather);
		directo = indexer(directo, withWeather, pipe);
		directo = featureSelection(directo, withWeather);
		directo = loadModel(ss, directo, model, model_name);

		// flight with one stop
		// Flight V1
		escala = preprocessing(ss, escala, withWeather);
		escala = indexer(escala, withWeather, pipe);
		escala = featureSelection(escala, withWeather);
		escala = loadModel(ss, escala, model, model_name);

		// drop temporal variables
		long ttt = System.currentTimeMillis();
		directo = drop(directo);
		escala = drop(escala);
		// System.out.println("tiem drop = " + (System.currentTimeMillis() - ttt));
		// Change names
		long tt = System.currentTimeMillis();
		directo = changeNames(directo);
		escala = changeNames(escala);
		// System.out.println("time change names=" + (System.currentTimeMillis() - tt));
		// Flight V2
		escala = preprocessing(ss, escala, withWeather);
		escala = indexer(escala, withWeather, pipe);
		escala = featureSelection(escala, withWeather);
		escala = loadModel(ss, escala, model, model_name);

		// drop temporal variables
		escala = drop(escala);

		// add missing columns to direct dataset
		directo = directo.withColumn("departureMonth_v1", functions.lit(null));
		directo = directo.withColumn("arrivalMonth_v1", functions.lit(null));
		directo = directo.withColumn("departureDay_v1", functions.lit(null));
		directo = directo.withColumn("arrivalDay_v1", functions.lit(null));
		directo = directo.withColumn("rawPrediction", functions.lit(null));
		// directo = directo.withColumn("probability", functions.lit(null));
		directo = directo.withColumn("prediction", functions.lit(null));

		out = directo.union(escala);
		// System.out.println("Pre-procession time = " + (System.currentTimeMillis() -
		// t1));

		if (!out.isEmpty()) {
			out = processiong4S(ss, out);
			/*
			 * Get column s to sort flight. First, filter flight with no prices and then,
			 * calculate S
			 */

			Dataset<Row> withPrices = out.filter("precio != \"Info\"");
			Dataset<Row> withOutPrices = out.filter("precio = \"Info\"");

			withPrices = processiong4S_2(ss, withPrices);
			// Drop temporal variables
			withPrices = withPrices.drop("precioSTD", "duracionSTD", "retrasoSTD", "precioNormalized",
					"duracionNormalized", "retrasoNormalized");

			// add missing columns to withOutPrices
			withOutPrices = withOutPrices.withColumn("precioMean", functions.lit(null))
					.withColumn("duracionMean", functions.lit(null)).withColumn("retrasoMean", functions.lit(null))
					.withColumn("S", functions.lit(1000.0));
			out = withPrices.union(withOutPrices);

			out = out.orderBy(out.col("S").asc());
		}
		return out;
	}
}
