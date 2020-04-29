package OnTime.routeStatusSpark;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;


public class Main 
{
	static String HADOOP_COMMON_PATH = "C:/Users/adilazh1/Documents/Universidad/Master/Laboratorio/Spark/SparkStream/Sesion_01/SparkStreamingI/src/main/resources/winutils";
	
    public static void main( String[] args ) throws IOException
    {
    	System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
    	long startTime = System.currentTimeMillis();
		/*
		 * First, read properties file, and initialize variables
		 */
		String appId = "";
		String appKey = "";
		String departureAirport = "";
		String arrivalAirport = "";
		Integer year = -1;
		Integer month = -1;
		Integer day = -1;
		Integer hourOfDay = -1;
		Integer numHours = -1;
		String reformat = "";
		String untilYesterday = "";
		//ArrayList<String> route_list = new ArrayList<String>();     
		
		// log4j
		Logger log = LogManager.getRootLogger();
		
		//auxiliar variables
		Calendar calendar = Calendar.getInstance();
		String route = "";
		// "yyyy-mm-dd"
		ArrayList<String> dates = new ArrayList<String>();
		Integer year_aux = -1;
		Integer month_aux = -1;
		Integer day_aux = -1;
		String date = "";
		
		String data_path = "";
		String DBdatabase = "";
		String DBcollection = "";

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));

			appId = properties.getProperty("appId");
			appKey = properties.getProperty("appKey");
			departureAirport = properties.getProperty("departureAirport");
			arrivalAirport = properties.getProperty("arrivalAirport");
			untilYesterday = properties.getProperty("untilYesterday");
			
			data_path = properties.getProperty("data_path");

			try {
				year = Integer.parseInt(properties.getProperty("year"));
				month = Integer.parseInt(properties.getProperty("month"));
				day = Integer.parseInt(properties.getProperty("day"));
				hourOfDay = Integer.parseInt(properties.getProperty("hourOfDay"));
				numHours = Integer.parseInt(properties.getProperty("numHours"));

				reformat = properties.getProperty("reformat");
				
				DBdatabase = properties.getProperty("database");
				DBcollection = properties.getProperty("collection");
						
			} catch (NumberFormatException e) {
				System.out.println("Error, check the properties file");
				System.out.println(e.getMessage());
			}

		} catch (FileNotFoundException e) {
			System.out.println("Error, propeties file don't existe");
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println("Error, can't read properties file");
			System.out.println(e.getMessage());
		}	
	
		if (year.equals( -1 ) || month.equals( -1 ) || day.equals( -1 ) || hourOfDay.equals( -1 )) {

			hourOfDay = 0;
			calendar.add(Calendar.DATE, -1);
			year = calendar.get(Calendar.YEAR);
			month = calendar.get(Calendar.MONTH) + 1;
			day = calendar.get(Calendar.DAY_OF_MONTH);

			date = year + "-" + month + "-" + day;
			dates.add(date);

		} else {
			if (untilYesterday.equals("Y")) {
				hourOfDay = 0;
				int i = -1;
				int j = 0;
				do {
					j++;
					Calendar calendar_aux = Calendar.getInstance();
					calendar_aux.add(Calendar.DATE, -j);
					year_aux = calendar_aux.get(Calendar.YEAR);
					month_aux = calendar_aux.get(Calendar.MONTH) + 1;
					day_aux = calendar_aux.get(Calendar.DAY_OF_MONTH);

					date = year_aux + "-" + month_aux + "-" + day_aux;
					dates.add(date);
					
					if (year_aux.equals(year)  && month_aux.equals(month) && day_aux.equals(day)) {
						i++;
					}
				}while(i < 0);
				

			} else {
				date = year + "-" + month + "-" + day;
				dates.add(date);
			}
		}

		//Start context Spark
		JavaSparkContext context = Utils.ContextStart(DBdatabase,DBcollection);
		
		Integer dates_size = dates.size();
		for (int j = 0 ; j < dates_size ; j++) {
			date = dates.get(j);
			year = Integer.parseInt(date.split("-")[0]);
			month = Integer.parseInt(date.split("-")[1]);
			day = Integer.parseInt(date.split("-")[2]);	
			//dowmload data and insert into mongoDB
			Spark.run(context, appId, appKey, year, month, day, hourOfDay, reformat,data_path);
			
		}
		
		long finishTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time = " + finishTime);
		context.close();
    }
}
