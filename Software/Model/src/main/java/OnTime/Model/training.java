package OnTime.Model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.ml.Model;
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.tree.impurity.Impurities;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.riversun.ml.spark.FeatureImportance;
import org.riversun.ml.spark.FeatureImportance.Order;
import org.riversun.ml.spark.Importance;

public class training {

	/**
	 * Adjust a linear SVM model using CV by selecting the best parameter C
	 * 
	 * @param training   dataset
	 * @param multiclass multiclassification Y or N
	 * @return CrossValidatorModel
	 */
	public static CrossValidatorModel fitModel(Dataset<Row> train, String multiclass) {
		CrossValidatorModel cvModel;
		// ToDo: Ajustar un modelo de clasificaciÃ³n binaria mediante CV
		// IndicaciÃ³n: Utilizar la Clase CrossValidator especificando el
		// estimador que hayais seleccionado
		LinearSVC lsvc = new LinearSVC().setMaxIter(30).setLabelCol("label").setFeaturesCol("features");
		if (!multiclass.equals("Y")) {
			lsvc.setWeightCol("classWeightCol");
		}

		ParamMap[] paramGrid = new ParamGridBuilder().addGrid(lsvc.regParam(), new double[] { 4.0, 0.2, 0.1 }).build();

		CrossValidator cv = new CrossValidator()
				.setEstimator(lsvc).setEvaluator(new MulticlassClassificationEvaluator().setMetricName("accuracy")
						.setLabelCol("label").setPredictionCol("prediction"))
				.setEstimatorParamMaps(paramGrid).setNumFolds(15);

		cvModel = cv.fit(train);

		return (cvModel);
	}

	/**
	 * Adjust a linear regression model using CV by selecting the best parameter
	 * 
	 * @param traininig  dataset
	 * @param multiclass Y or N
	 * @return CrossValidatorModel
	 */
	public static CrossValidatorModel fitModelLR(Dataset<Row> train, String multiclass) {
		CrossValidatorModel cvModel;
		// ToDo: Ajustar un modelo de clasificaciÃ³n binaria mediante CV
		// IndicaciÃ³n: Utilizar la Clase CrossValidator especificando el
		// estimador que hayais seleccionado

		LogisticRegression lr = new LogisticRegression().setTol(1E-6).setLabelCol("label").setFeaturesCol("features");

		if (!multiclass.equals("Y")) {
			lr.setWeightCol("classWeightCol");
		}
		ParamMap[] paramGrid = new ParamGridBuilder()
				.addGrid(lr.regParam(), new double[] { 4.0,2.0, 0.3, 0.1})
				.addGrid(lr.elasticNetParam(), new double[] { 0.0,0.5,1.0})
				.addGrid(lr.maxIter(), new int[] {10,50,100})
				.build();

		CrossValidator cv = new CrossValidator()
				.setEstimator(lr).setEvaluator(new MulticlassClassificationEvaluator().setMetricName("accuracy")
						.setLabelCol("label").setPredictionCol("prediction"))
				.setEstimatorParamMaps(paramGrid).setNumFolds(15);

		cvModel = cv.fit(train);

		return (cvModel);
	}

	/**
	 * Main class of training model java class
	 * 
	 * @param ss         SparkSesion
	 * @param File       dataset
	 * @param model      model's name
	 * @param model_path path to save model
	 * @param weather    Y or N
	 * @param multiclass Y or N
	 * @throws IOException
	 */
	public static void trainingModel(SparkSession ss, Dataset<Row> File, String model, String model_path,
			String weather, String multiclass) throws IOException {
		// Training a model

		Dataset<Row>[] splits = File.randomSplit(new double[] { 0.25, 0.75 });
		Dataset<Row> train = splits[1];
		Dataset<Row> test = splits[0];
		train.persist();
		test.persist();
		Integer train_size = (int) train.count();
		Integer test_size = (int) test.count();

		// Auxiliary variables
		// Save  model
		System.out.println("model training");
		Calendar calendar = Calendar.getInstance();
		Integer year = calendar.get(Calendar.YEAR);
		Integer month = calendar.get(Calendar.MONTH) + 1;
		Integer day = calendar.get(Calendar.DAY_OF_MONTH);
		String date = year + "-" + month + "-" + day;
		String path_save = "";
		Dataset<Row> predictions = null;

		// Save the output for the statistics
		FileWriter FW = new FileWriter(model_path + "statistics.csv", true);
		BufferedWriter B = new BufferedWriter(FW);

		long t = System.currentTimeMillis();
		if (model.equals("RF")) {

			RandomForestClassificationModel rf = new RandomForestClassifier().setLabelCol("label")
					.setFeaturesCol("features").setPredictionCol("prediction").setRawPredictionCol("rawPrediction")
					.setProbabilityCol("probability")
					// .setMaxBins(60000)
					.setNumTrees(300)
					// .setImpurity("gini")
					.setImpurity("entropy").setSeed(13L).fit(train);

			predictions = rf.transform(test);

			// Get schema from result DataSet
			StructType schema = predictions.schema();

			// Get sorted feature importance with column name
			List<Importance> importanceList = new FeatureImportance.Builder(rf, schema).sort(Order.DESCENDING).build()
					.getResult();

			Iterator<Importance> importanceFeatures = importanceList.iterator();
			// Writer
			FileWriter fw = new FileWriter(model_path + "featureImportance_RF.txt");
			BufferedWriter b = new BufferedWriter(fw);
			while (importanceFeatures.hasNext()) {
				String importanceF = importanceFeatures.next().toString();
				b.write(importanceF + "\n");
			}

			b.close();
			predictions = predictions.select("prediction", "label");
			path_save = model_path + "RandomForest_" + date + "_" + weather;
			rf.save(path_save);

		}

		if (model.equals("RF_CV")) {

			RandomForestClassifier rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setImpurity("gini").setCacheNodeIds(true);

			ParamMap[] paramGrid = new ParamGridBuilder().addGrid(rf.maxBins(), new int[] { 100, 200 })
					.addGrid(rf.maxDepth(), new int[] { 2, 4, 10 }).addGrid(rf.numTrees(), new int[] { 50, 100, 200 })
					//.addGrid(rf.impurity(),"entropy")
					.build();

			BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
					.setRawPredictionCol("prediction");

			CrossValidator cv = new CrossValidator().setEstimator(rf).setEvaluator(evaluator)
					.setEstimatorParamMaps(paramGrid).setNumFolds(10);
			CrossValidatorModel cvModel;
			cvModel = cv.fit(train);
			predictions = cvModel.transform(test).select("prediction", "label");

			path_save = model_path + "RF_CV" + date + "_" + weather;
			cvModel.save(path_save);

			// best model
			System.out.println("RF_CV best model parameters:" + cvModel.bestModel().explainParams());

		}

		if (model.equals("GBTC")) {
			// Train a GBT model.
			GBTClassifier gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(100).setImpurity("gini").setSeed(13L)
					;

			//System.out.println(gbt.paramMap());
			GBTClassificationModel gbtModel = gbt.fit(train);

			// Make predictions.

			predictions = gbtModel.transform(test);

			// Get schema from result DataSet
			StructType schema = predictions.schema();

			// Get sorted feature importances with column name
			List<Importance> importanceList = new FeatureImportance.Builder(gbtModel, schema).sort(Order.DESCENDING)
					.build().getResult();

			Iterator<Importance> importanceFeatures = importanceList.iterator();
			// Writer
			FileWriter fw = new FileWriter(model_path + "featureImportance_GBTC.txt");
			BufferedWriter b = new BufferedWriter(fw);
			while (importanceFeatures.hasNext()) {
				String importanceF = importanceFeatures.next().toString();
				b.write(importanceF + "\n");
			}

			b.close();
			predictions = predictions.select("prediction", "label");
			path_save = model_path + "GBTC" + date + "_" + weather;
			gbtModel.save(path_save);

		}

		if (model.equals("LinearSVM")) {
			// Ajustamos un modelo de clasificaciÃ³n binaria con CV
			CrossValidatorModel cvModel = fitModel(train, multiclass);

			predictions = cvModel.transform(test).select("prediction", "label");

			path_save = model_path + "LinearSVM_" + date + "_" + weather;
			cvModel.save(path_save);
		}

		if (model.equals("OneVsRestLR")) {

			// train.show(5);
			LogisticRegression classifier = new LogisticRegression().setMaxIter(100).setTol(1E-6).setFitIntercept(true);

			if (!multiclass.equals("Y")) {
				classifier.setWeightCol("classWeightCol");
			}
			// instantiate the One Vs Rest Classifier.
			OneVsRest ovr = new OneVsRest().setClassifier(classifier);
			if (!multiclass.equals("Y")) {
				ovr.setWeightCol("classWeightCol");
			}

			// train the multiclass model.
			OneVsRestModel ovrModel = ovr.fit(train);
			// score the model on test data.
			predictions = ovrModel.transform(test).select("prediction", "label");

			path_save = model_path + "OneVsRestLR_" + date + "_" + weather;
			ovrModel.save(path_save);

		}
		if (model.equals("OneVsRestSVM")) {

			LinearSVC lsvc = new LinearSVC().setMaxIter(250).setTol(1E-6).setLabelCol("label")
					.setFeaturesCol("features").setStandardization(true);
			if (!multiclass.equals("Y")) {
				lsvc.setWeightCol("classWeightCol");
			}
			// instantiate the One Vs Rest Classifier.
			OneVsRest ovr = new OneVsRest().setClassifier(lsvc);
			if (!multiclass.equals("Y")) {
				ovr.setWeightCol("classWeightCol");
			}
			// train the multiclass model.
			OneVsRestModel ovrModel = ovr.fit(train);
			// score the model on test data.
			predictions = ovrModel.transform(test).select("prediction", "label");

			path_save = model_path + "OneVsRestSVM_" + date + "_" + weather;
			ovrModel.save(path_save);
			

		}

		if (model.equals("LR")) {
			CrossValidatorModel lrModel = fitModelLR(train, multiclass);

			predictions = lrModel.transform(test).select("prediction", "label");

			path_save = model_path + "LogesticRegression_" + date + "_" + weather;
			lrModel.save(path_save);
			
			// ROC curve
			LogisticRegressionModel a = (LogisticRegressionModel) lrModel.bestModel();
			LogisticRegressionTrainingSummary trainingSummary = a.summary();
			// Obtain the metrics useful to judge performance on test data.
			// We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
			// classification problem.
			BinaryLogisticRegressionSummary binarySummary =
			  (BinaryLogisticRegressionSummary) trainingSummary;

			// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
			Dataset<Row> roc = binarySummary.roc();
			//roc.show();
			//roc.select("FPR").show();
			System.out.println("ROC area ="+binarySummary.areaUnderROC());
		}

		if (model.equals("MLPC")) {
			int[] layers = new int[] { 14, 7, 3, 2 };

			// create the trainer and set its parameters
			MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier().setLayers(layers)
					.setBlockSize(128).setSeed(1234L).setMaxIter(200).setFeaturesCol("features").setLabelCol("label")
					.setPredictionCol("prediction").setProbabilityCol("probabilty");

			// train the model
			train = train.select("features", "label");
			MultilayerPerceptronClassificationModel MLPCmodel = trainer.fit(train);

			// compute accuracy on the test set
			predictions = MLPCmodel.transform(test).select("prediction", "label");

			path_save = model_path + "MultilayerPerceptron_" + date + "_" + weather;
			MLPCmodel.save(path_save);

			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
					.setMetricName("accuracy");

			System.out.println(predictions.toJavaRDD().take(5));
			System.out.println("Test set accuracy = " + evaluator.evaluate(predictions));
		}

		if (model.equals("NB")) {
			// create the trainer and set its parameters
			NaiveBayes nb = new NaiveBayes().setLabelCol("label").setFeaturesCol("features")
					.setPredictionCol("prediction");

			if (!multiclass.equals("Y")) {
				nb.setWeightCol("classWeightCol");
			}
			// train the model
			NaiveBayesModel NBmodel = nb.fit(train);

			// Select example rows to display.
			predictions = NBmodel.transform(test).select("prediction", "label");

			// compute accuracy on the test set
			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
					.setPredictionCol("prediction").setMetricName("accuracy");
			double accuracy = evaluator.evaluate(predictions);
			System.out.println("Test set accuracy = " + accuracy);

			path_save = model_path + "NaiveBayes_" + date + "_" + weather;
			NBmodel.save(path_save);
			
		}

		/*
		 * Confusion matrix and precision metrics
		 */

		if (!model.equals("MLPC")) {
			System.out.println("Predictions:");
			//predictions.show(10);
			MulticlassMetrics mm = new MulticlassMetrics(predictions.toDF());

			System.out.println("The used model is :" + model + " option weather :" + weather);
			System.out.println(mm.confusionMatrix().toString());
			System.out.println("Accuracy %= " + 100 * mm.accuracy());
			int j = mm.labels().length;
			for (int i = 0; i < j; i++) {
				Double label = mm.labels()[i];
				System.out.println("Recall label " + label + " = " + 100 * mm.recall(label));
			}
			if (!multiclass.equals("Y")) {
				B.write("\n"+date + ";" + model + ";" + weather + ";"
						+ String.format("%.3f", 100 * mm.accuracy()) + ";"
						+ String.format("%.3f", 100 * mm.recall(mm.labels()[0])) + ";"
						+ String.format("%.3f", 100 * mm.recall(mm.labels()[1]))+ ";"
						+ ((System.currentTimeMillis() - t) / 1000));
			}
		}

		System.out.println("Train samples: " + train_size);
		System.out.println("Test samples: " + test_size);
		B.close();
		
		
		//ATTENTION: NO DELETE THIS COMENTED CODE
		/*
		 * A new method to compare performance between our algorithms and a simple discriminate function
		 * using delayMean of flight giving by routeRatings, Mr Dedicado wont to see this comparison 
		 */
		
		/*
		System.out.println("##################################################################");
		System.out.println("Algorithm using delayMean and discriminate function");
		Dataset<Row> test2 = addPredictionOfRatings(ss,test);
		Dataset<Row> PredictionOfRatings = test2.select("PredictionOfRatings","label");
		MulticlassMetrics mm2 = new MulticlassMetrics(PredictionOfRatings.toDF());

		System.out.println(mm2.confusionMatrix().toString());
		System.out.println("Accuracy %= " + 100 * mm2.accuracy());
		int j = mm2.labels().length;
		for (int i = 0; i < j; i++) {
			Double label = mm2.labels()[i];
			System.out.println("Recall label " + label + " = " + 100 * mm2.recall(label));
		}
		*/
		
	}
	
	public static Dataset<Row> addPredictionOfRatings(SparkSession ss,Dataset<Row> data){
		
		ss.udf().register("getPrediction", new UDF1<Float, Double>() {
			private static final long serialVersionUID = 1L;

			Double prediction = null;
			public Double call(final Float delayMean) throws ParseException {

				if(delayMean != null) {
					if(delayMean < 15.0) {
						prediction = 0.0;
					}else {
						prediction = 1.0;
					}
				}
			
				return prediction;
			}
		}, DataTypes.DoubleType);
		
		data = data.withColumn("PredictionOfRatings", functions.callUDF("getPrediction", data.col("delayMean_v1")));
		return data;
	}
}
