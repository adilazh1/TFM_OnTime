package OnTime.Model;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import java.util.Locale;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class processing {

	/**
	 * Method to categorize a giving date: month, day and "moment of day"
	 * 
	 * @param ss   SparkSesion
	 * @param data Dataset<Row>
	 * @return Dataset<Row>
	 * @throws ParseException
	 */
	public static Dataset<Row> categorizeDate(SparkSession ss, Dataset<Row> data) throws ParseException {

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
				String date = departureDate.split("T")[0];
				String month = date.split("-")[1];
				departureMonth = months.get(month);
				return departureMonth;
			}
		}, DataTypes.StringType);

		// Categorize day
		ss.udf().register("getDay", new UDF1<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(final String departureDate) throws ParseException {

				String departureDay = null;
				String date = departureDate.split("T")[0];
				departureDay = getDay(date);
				return departureDay;
			}
		}, DataTypes.StringType);

		// Categorize moment of day. Day is partitioned into 24/3 categories
		ss.udf().register("getDayMoment", new UDF1<String, String>() {
			private static final long serialVersionUID = 1L;

			public String call(final String departureDate) throws ParseException {

				String dayMoment = null;
				String hour = departureDate.split("T")[1].substring(0, 5);
				Integer h = Integer.valueOf(hour.split(":")[0]);
				Integer m = Integer.valueOf(hour.split(":")[1]);
				Integer M = (h * 60) + m;
				Integer mod = M / 180;

				dayMoment = "d" + mod;
				return dayMoment;
			}
		}, DataTypes.StringType);

		data = data.withColumn("departureMonth_v1", functions.callUDF("getMonth", data.col("departureDate")));
		data = data.withColumn("arrivalMonth_v1", functions.callUDF("getMonth", data.col("arrivalDate")));
		data = data.withColumn("departureDay_v1", functions.callUDF("getDay", data.col("departureDate")));
		data = data.withColumn("arrivalDay_v1", functions.callUDF("getDay", data.col("arrivalDate")));
		data = data.withColumn("departureDayMoment_v1", functions.callUDF("getDayMoment", data.col("departureDate")));
		data = data.withColumn("arrivalDayMoment_v1", functions.callUDF("getDayMoment", data.col("arrivalDate")));

		return data;

	}

	/**
	 * Method to categorize delay variable and same weather's variables
	 * 
	 * @param ss         SparkSesion
	 * @param data
	 * @param multiclass Y= multiclass classification, N=binary classification
	 * @return Dataset<Row>
	 */
	public static Dataset<Row> categorizeDelayAndWeather(SparkSession ss, Dataset<Row> data, String multiclass) {

		// Categorize

		double[] splits;

		if (multiclass.equals("Y")) {
			splits = new double[] { 0.0, 15.0, 30.0, 45.0, 60.0, Double.MAX_VALUE };
		} else {
			splits = new double[] { 0.0, 15.0, Double.MAX_VALUE };
		}

		Bucketizer descret = new Bucketizer().setInputCol("departureGateDelayMinutes").setOutputCol("delay")
				.setSplits(splits);

		double[] splits1 = { 0.0, 15.0, 30.0, 45.0, 60.0, Double.MAX_VALUE };
		Bucketizer descret1 = new Bucketizer().setInputCol("delayMean_v1").setOutputCol("delayMeanLabel_v1")
				.setSplits(splits);

		// Categorize some other variables relative to temperature and statistics
		// Categorize Temperature. Giving in Kelving
		double[] split2 = { 0.0, 268.0, 308.0, Double.MAX_VALUE };
		Bucketizer descret2 = new Bucketizer().setInputCol("temp_v1").setOutputCol("tempLabel_v1").setSplits(split2);

		// Categorize wind deg in 18 categories
		double[] split3 = { 0.0, 20.0, 40.0, 60.0, 80.0, 100.0, 120.0, 140.0, 160.0, 180.0, 200.0, 220.0, 240.0, 260.0,
				280.0, 300.0, 320.0, 340.0, Double.MAX_VALUE };
		Bucketizer descret3 = new Bucketizer().setInputCol("windDeg_v1").setOutputCol("windDegLabel_v1")
				.setSplits(split3);

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { descret1, descret, descret2, descret3 });

		PipelineModel model = pipeline.fit(data);
		data = model.transform(data);

		return data;

	}

	/**
	 * Feature selection
	 * 
	 * @param data
	 * @param weather
	 * @return Dataset
	 */
	public static Dataset<Row> featureSelection(Dataset<Row> data, String weather) {

		if (weather.equals("Y")) {

			String[] features = { "departureAirportOneHot", "arrivalAirportOneHot", "carrierFsCodeOneHot", "tempOneHot",
					"humidity", "pressure", "wind_degOneHot", "weatherOneHot", "wind_speed", "departureMonthOneHot",
					"arrivalMonthOneHot", "departureDayOneHot", "arrivalDayOneHot", "departureDayMomentOneHot",
					"arrivalDayMomentOneHot", "delayMeanLabelOneHot", "allOntimeCumulative_v1",
					"allDelayCumulative_v1" };
					
			VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("features");
			data = assembler.transform(data);

			// System.out.println("With weather, number of variables=" + features.length);
		} else {

			String[] features = { "departureAirportOneHot", "arrivalAirportOneHot", "carrierFsCodeOneHot",
					"departureMonthOneHot", "arrivalMonthOneHot", "departureDayOneHot", "arrivalDayOneHot",
					"departureDayMomentOneHot", "arrivalDayMomentOneHot", "delayMeanLabelOneHot",
					"allOntimeCumulative_v1",  "allDelayCumulative_v1" };

			VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("features");
			data = assembler.transform(data);
			// System.out.println("Without weather, number of variables=" +
			// features.length);
		}

		return data;

	}

	/**
	 * StringIndexerModel for Categorical variables
	 * 
	 * @param data       input
	 * @param weather    Y for algorithms include weather data N otherwise
	 * @param Model      name of model, to include it in the path
	 * @param model_path where to save indexes
	 * @return dataset
	 * @throws IOException
	 */
	public static Dataset<Row> indexer(Dataset<Row> data, String weather, String Model, String model_path)
			throws IOException {

		StringIndexerModel departureAirportIndx_v1 = new StringIndexer().setInputCol("departureAirportFsCode_v1")
				.setOutputCol("departureAirportIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel arrivalAirportIndx_v1 = new StringIndexer().setInputCol("arrivalAirportFsCode_v1")
				.setOutputCol("arrivalAirportIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel carrierFsCodeIndx_v1 = new StringIndexer().setInputCol("carrierFsCode_v1")
				.setOutputCol("carrierFsCodeIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel tempIndx_v1 = new StringIndexer().setInputCol("tempLabel_v1").setOutputCol("tempIndx_v1")
				.setHandleInvalid("skip").fit(data);
		StringIndexerModel weatherIndx_v1 = new StringIndexer().setInputCol("weather_v1").setOutputCol("weatherIndx_v1")
				.setHandleInvalid("skip").fit(data);
		StringIndexerModel windDegIndx_v1 = new StringIndexer().setInputCol("windDegLabel_v1")
				.setOutputCol("windDegIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel departureMonthIndx_v1 = new StringIndexer().setInputCol("departureMonth_v1")
				.setOutputCol("departureMonthIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel arrivalMonthIndx_v1 = new StringIndexer().setInputCol("arrivalMonth_v1")
				.setOutputCol("arrivalMonthIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel departureDayIndx_v1 = new StringIndexer().setInputCol("departureDay_v1")
				.setOutputCol("departureDayIndx_v1").setHandleInvalid("skip").setHandleInvalid("skip").fit(data);
		StringIndexerModel arrivalDayIndx_v1 = new StringIndexer().setInputCol("arrivalDay_v1")
				.setOutputCol("arrivalDayIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel departureDayMomentIndx_v1 = new StringIndexer().setInputCol("departureDayMoment_v1")
				.setOutputCol("departureDayMomentIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel arrivalDayMomentIndx_v1 = new StringIndexer().setInputCol("arrivalDayMoment_v1")
				.setOutputCol("arrivalDayMomentIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel delayMeanLabelIndx_v1 = new StringIndexer().setInputCol("delayMeanLabel_v1")
				.setOutputCol("delayMeanLabelIndx_v1").setHandleInvalid("skip").fit(data);
		StringIndexerModel label = new StringIndexer().setInputCol("delay").setOutputCol("label")
				.setHandleInvalid("skip").fit(data);

		Pipeline pipeline = new Pipeline();
		if (weather.equals("Y")) {

			pipeline = pipeline.setStages(new PipelineStage[] { departureAirportIndx_v1, arrivalAirportIndx_v1,
					carrierFsCodeIndx_v1, tempIndx_v1, weatherIndx_v1, windDegIndx_v1, departureMonthIndx_v1,
					arrivalMonthIndx_v1, departureDayIndx_v1, arrivalDayIndx_v1, departureDayMomentIndx_v1,
					arrivalDayMomentIndx_v1, delayMeanLabelIndx_v1 });

		} else {

			pipeline = pipeline.setStages(new PipelineStage[] { departureAirportIndx_v1, arrivalAirportIndx_v1,
					carrierFsCodeIndx_v1, departureMonthIndx_v1, arrivalMonthIndx_v1, departureDayIndx_v1,
					arrivalDayIndx_v1, departureDayMomentIndx_v1, arrivalDayMomentIndx_v1, delayMeanLabelIndx_v1});

		}

		PipelineModel model = pipeline.fit(data);

		Calendar calendar = Calendar.getInstance();
		Integer year = calendar.get(Calendar.YEAR);
		Integer month = calendar.get(Calendar.MONTH) + 1;
		Integer day = calendar.get(Calendar.DAY_OF_MONTH);
		String date = year + "-" + month + "-" + day;

		// Save Indexes
		String path = model_path + "Index/" + Model + date + "_" + "Indx" + "-" + weather;
		pipeline.save(path);
		data = label.transform(data);
		data = model.transform(data);

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
	 * Balance data in case binary classification
	 * 
	 * @param ss      SparkSesion
	 * @param Dataset
	 * @return Dataset
	 */
	public static Dataset<Row> balanceDataset(SparkSession ss, Dataset<Row> File) {
		// Re-balancing (weighting) of records to be used in the logistic loss objective
		// function
		long label0 = (int) File.filter("label=0.0").count();

		long datasetSize = (int) File.count();
		Double balancingRatio = Double.valueOf(datasetSize - label0) / datasetSize;

		ss.udf().register("getBalance", new UDF1<Double, Double>() {
			private static final long serialVersionUID = 1L;

			public Double call(final Double label) throws ParseException {
				Double ratio = null;
				if (label.equals(0.0)) {
					ratio = 1 * balancingRatio;
				} else {
					ratio = (1 * (1.0 - balancingRatio));
				}

				return ratio;
			}
		}, DataTypes.DoubleType);

		File = File.withColumn("classWeightCol", functions.callUDF("getBalance", File.col("label")));

		return File;

	}

	public static int getOrder(double l0, double l1, double l2, double l3, double l4) {
		int order = 0;
		double[] aux = new double[] { l0, l1, l2, l3, l4 };
		double aux2 = aux[0];
		for (int i = 0; i < 5; i++) {
			if (aux[i] < aux2) {
				order = i;
				aux2 = aux[i];
			}
		}
		return order;
	}

	/**
	 * Main class of this java class
	 * 
	 * @param ss         SparkSesion
	 * @param File       Dataset
	 * @param weather    weather Y or Weather N
	 * @param Model      name of the model to training
	 * @param multiclass Multiclass Y or Binary classification N
	 * @return Dataset
	 * @throws ParseException
	 * @throws IOException
	 */
	public static Dataset<Row> preprocessing(SparkSession ss, Dataset<Row> File, String weather, String Model,
			String multiclass, String model_path) throws ParseException, IOException {

		System.out.println("Preprocesing");
		File = File.drop("departuredateUtc", "flightNumber", "day", "month", "year", "arrivalDateUtc", "flightType",
				"ontime", "late15", "late30", "late45", "delayStandardDeviation", "delayMin", "delayMax");
		// System.out.println(File.count());
		File = File.na().drop(); // only one row removed!!!
		// System.out.println(File.count());
		File = categorizeDate(ss, File);
		File = categorizeDelayAndWeather(ss, File, multiclass);
		
		// Save File to analysis
//		File.select("departureAirportFsCode_v1", "arrivalAirportFsCode_v1", "carrierFsCode_v1", "departureDate",
//				"arrivalDate", "departureGateDelayMinutes", "pressure", "weather_v1", "wind_speed", "windDeg_v1",
//				"humidity", "delayMean_v1", "allOntimeCumulative_v1", "allOntimeStars_v1", "allDelayCumulative_v1",
//				"allDelayStars_v1", "departureMonth_v1", "arrivalMonth_v1", "departureDay_v1", "arrivalDay_v1",
//				"departureDayMoment_v1", "arrivalDayMoment_v1", "delayMeanLabel_v1", "delay", "tempLabel_v1",
//				"windDegLabel_v1", "allOntimeStarsLabel_v1", "allDelayStarsLabel_v1")
//		.coalesce(1)
//		.write().option("header", true).option("delimiter", ";")
//		.csv("dataset_procesado.csv");
//		System.out.println("*** csv saved ***");

		File = indexer(File, weather, Model, model_path);
		// File.sa
		
		OneHotEncoderEstimator oneHot;
		if (weather.equals("Y")) {
			oneHot = new OneHotEncoderEstimator()

					.setInputCols(new String[] { "departureAirportIndx_v1", "arrivalAirportIndx_v1",
							"carrierFsCodeIndx_v1", "weatherIndx_v1", "tempIndx_v1", "windDegIndx_v1",
							"departureMonthIndx_v1", "arrivalMonthIndx_v1", "departureDayIndx_v1", "arrivalDayIndx_v1",
							"departureDayMomentIndx_v1", "arrivalDayMomentIndx_v1", "delayMeanLabelIndx_v1"})
					.setOutputCols(new String[] { "departureAirportOneHot", "arrivalAirportOneHot",
							"carrierFsCodeOneHot", "weatherOneHot", "tempOneHot", "wind_degOneHot",
							"departureMonthOneHot", "arrivalMonthOneHot", "departureDayOneHot", "arrivalDayOneHot",
							"departureDayMomentOneHot", "arrivalDayMomentOneHot", "delayMeanLabelOneHot" });
		} else {
			oneHot = new OneHotEncoderEstimator()

					.setInputCols(
							new String[] { "departureAirportIndx_v1", "arrivalAirportIndx_v1", "carrierFsCodeIndx_v1",
									"departureMonthIndx_v1", "arrivalMonthIndx_v1", "departureDayIndx_v1",
									"arrivalDayIndx_v1", "departureDayMomentIndx_v1", "arrivalDayMomentIndx_v1",
									"delayMeanLabelIndx_v1" })
					.setOutputCols(new String[] { "departureAirportOneHot", "arrivalAirportOneHot",
							"carrierFsCodeOneHot", "departureMonthOneHot", "arrivalMonthOneHot", "departureDayOneHot",
							"arrivalDayOneHot", "departureDayMomentOneHot", "arrivalDayMomentOneHot",
							"delayMeanLabelOneHot"});
		}

		File = oneHot.fit(File).transform(File);

		// feature selection
		File = featureSelection(File, weather);

		// balancing dataset

		Dataset<Row> File_out = null;
		/*
		 * For multiclass model, there are 5 classes, we implement a manual balance
		 */
		if (multiclass.equals("Y")) {
			Dataset<Row> File_0 = File.filter("label=0.0");
			Dataset<Row> File_1 = File.filter("label=1.0");
			Dataset<Row> File_2 = File.filter("label=2.0");
			Dataset<Row> File_3 = File.filter("label=3.0");
			Dataset<Row> File_4 = File.filter("label=4.0");
			double l0 = File_0.count();
			double l1 = File_1.count();
			double l2 = File_2.count();
			double l3 = File_3.count();
			double l4 = File_4.count();
			double r1 = 0.0;
			double r2 = 0.0;
			double r3 = 0.0;
			double r4 = 0.0;
			int reponse = getOrder(l0, l1, l2, l3, l4);
			switch (reponse) {
			case 0:
				r1 = 1.02 * (l0 / l1);
				r2 = 1.02 * (l0 / l2);
				r3 = 1.02 * (l0 / l3);
				r4 = 1.02 * (l0 / l4);
				File_1 = File_1.sample(false, r1, 36L);
				File_2 = File_2.sample(false, r2, 36L);
				File_3 = File_3.sample(false, r3, 36L);
				File_4 = File_4.sample(false, r4, 36L);
				File_out = File_0.union(File_1).union(File_2).union(File_3).union(File_4);
				break;
			case 1:
				r1 = 1.02 * (l1 / l0);
				r2 = 1.02 * (l1 / l2);
				r3 = 1.02 * (l1 / l3);
				r4 = 1.02 * (l1 / l4);
				File_0 = File_0.sample(false, r1, 36L);
				File_2 = File_2.sample(false, r2, 36L);
				File_3 = File_3.sample(false, r3, 36L);
				File_4 = File_4.sample(false, r4, 36L);
				File_out = File_0.union(File_1).union(File_2).union(File_3).union(File_4);
				break;

			case 2:
				r1 = 1.02 * (l2 / l0);
				r2 = 1.02 * (l2 / l1);
				r3 = 1.02 * (l2 / l3);
				r4 = 1.02 * (l2 / l4);
				File_0 = File_0.sample(false, r1, 36L);
				File_1 = File_1.sample(false, r2, 36L);
				File_3 = File_3.sample(false, r3, 36L);
				File_4 = File_4.sample(false, r4, 36L);
				File_out = File_0.union(File_1).union(File_2).union(File_3).union(File_4);
				break;

			case 3:
				r1 = 1.02 * (l3 / l0);
				r2 = 1.02 * (l3 / l1);
				r3 = 1.02 * (l3 / l2);
				r4 = 1.02 * (l3 / l4);
				File_0 = File_0.sample(false, r1, 36L);
				File_1 = File_1.sample(false, r2, 36L);
				File_2 = File_2.sample(false, r3, 36L);
				File_4 = File_4.sample(false, r4, 36L);
				File_out = File_0.union(File_1).union(File_2).union(File_3).union(File_4);
				break;

			case 4:
				r1 = 1.02 * (l4 / l0);
				r2 = 1.02 * (l4 / l1);
				r3 = 1.02 * (l4 / l2);
				r4 = 1.02 * (l4 / l3);
				File_0 = File_0.sample(false, r1, 36L);
				File_1 = File_1.sample(false, r2, 36L);
				File_2 = File_2.sample(false, r3, 36L);
				File_3 = File_3.sample(false, r4, 36L);
				File_out = File_0.union(File_1).union(File_2).union(File_3).union(File_4);
				break;

			default:

				break;
			}

		} else {
			/*
			 * At randomForests, there is no possibility to consider unbalanced classes
			 * automatically with the WeightCol column.
			 */

			if (Model.equals("RF") || Model.equals("RF_CV") || Model.equals("GBTC")) {
				Dataset<Row> File_0 = File.filter("label=0.0");
				Dataset<Row> File_1 = File.filter("label=1.0");
				double File_0_nb = File_0.count();
				double File_1_nb = File_1.count();
				// float tot = File.count();
				double ratio = 1.03 * (File_1_nb / File_0_nb);
				File_0 = File_0.sample(false, ratio, 36L);
				Dataset<Row> File_2 = File_0.union(File_1);
				File_out = File_2;
				/*
				 * For the others models, is possible a automated "balance" using weigtCol
				 */
			} else {
				File = balanceDataset(ss, File);
				File_out = File;
			}
		}
		
		//Statistics Active only if you wont have statistics 
//		File.printSchema();
//		statistics.get_corr(File,model_path);
//		statistics.get_chi(File,model_path);
		
		return File_out;

	}
}
