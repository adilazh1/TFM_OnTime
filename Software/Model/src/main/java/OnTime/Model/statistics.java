package OnTime.Model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class statistics {

	/**
	 * Write correlation table between numerical variables, in csv file
	 * @param data
	 * @param model_path
	 * @throws IOException
	 */
	public static void get_corr(Dataset<Row> data, String model_path) throws IOException {
		// correlation between features
		String[] features = { "temp_v1", "pressure", "wind_speed", "windDeg_v1", "humidity", "delayMean_v1",
				"allOntimeCumulative_v1","allOntimeStars_v1", "allDelayCumulative_v1", "allDelayStars_v1" };

		VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("features");
		data = assembler.transform(data);

		Dataset<Row> corr = Correlation.corr(data, "features");

		Row r1 = corr.head();
		Matrix m = r1.getAs(0);
		// Writer
		FileWriter fw = new FileWriter(model_path + "correlation.csv");
		BufferedWriter b = new BufferedWriter(fw);
		String[] header = { "temp;", "pressure;", "wind_speed;", "windDeg;", "humidity;",
				"delayMean;" , "allOntimeC;","allOntimeS;", "allDelayC;", "allDelayS;" };
		
		String head = " ;" + "temp;"+ "pressure;"+ "wind_speed;"+ "windDeg;"+ "humidity;"+
				"delayMean;" + "allOntimeC;"+"allOntimeS;"+ "allDelayC;"+ "allDelayS"+ "\n";
		b.write(head);

		for (int i = 0; i < m.numRows(); i++) {
			
			b.write(header[i].toString());
			for (int j = 0; j < m.numCols(); j++) {
				double aa = m.apply(i, j);
				b.write(String.format("%.3f", aa) + ";");
			}
			b.write("\n");
		}

		b.close();

	}

	public static void get_chi(Dataset<Row> data, String model_path) throws IOException {

		// Chi square test
		String[] features = { "departureAirportIndx_v1", "arrivalAirportIndx_v1","carrierFsCodeIndx_v1",
				"departureMonthIndx_v1","arrivalMonthIndx_v1","departureDayIndx_v1","arrivalDayIndx_v1",
				"departureDayMomentIndx_v1","arrivalDayMomentIndx_v1"};

		VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("features");
		data = assembler.transform(data);

		Dataset<Row> chi = ChiSquareTest.test(data, "features", "label");
		Row r = chi.head();
		// Writer
//		FileWriter fw = new FileWriter(model_path + "chi.csv");
//		BufferedWriter b = new BufferedWriter(fw);

		System.out.println("Chi square independence test.Categorial variables vs Reponse variable");
		System.out.println("departureAirpor,arrivalAirpor,carrierFsCode,departureMonth,arrivalMonth,departureDay,arrivalDay,departureDayMoment,arrivalDayMoment");
		System.out.println("pValues: " + r.get(0).toString());
		System.out.println("degreesOfFreedom: " + r.getList(1).toString());
		System.out.println("statistics: " + r.get(2).toString());
		
//		b.close();

	}
}
