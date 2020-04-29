package OnTime.routeStatus;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Main {
	public static void main(String[] args) throws IOException {
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
		ArrayList<String> route_list = new ArrayList<String>();
		FileWriter writer = new FileWriter("routesWithoutFlights",true);
		BufferedWriter b = new BufferedWriter(writer);

		// log4j
		Logger log = LogManager.getRootLogger();

		// Auxiliary variables
		String request = "";
		String _id = "";
		boolean to_insert = false;
		Calendar calendar = Calendar.getInstance();
		String route = "";
		// "yyyy-mm-dd"
		ArrayList<String> dates = new ArrayList<String>();
		String departureAirport_aux = "";
		String arrivalAirport_aux = "";
		Integer year_aux = -1;
		Integer month_aux = -1;
		Integer day_aux = -1;
		String date = "";
		Integer routes_size = 0;
		String routeIgnored = "";
		String data_path  = "";
		String DBdatabase = "";
		String DBcollection = "";
		
		log.info("routeStatus >> Application started normally");

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));
			//properties.load(new FileInputStream("C:/Users/adilazh1/eclipse-workspace/parametrs/routetStatus.properties"));

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

		// MongoDB client
		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase(DBdatabase);
		MongoCollection<Document> collection = database.getCollection(DBcollection);
		
		/*
		 * *****************************************************************************
		 * ******** If some parameter airport is empty, load routes's IATA codes *******
		 * *****************************************************************************
		 */

		if (departureAirport.equals("") || arrivalAirport.equals("")) {

			try {
				route_list = LoadRoutesIATA.get(data_path);
			    routes_size = route_list.size();
				// route_list =LoadRoutesIATA.get("src/main/resources/routes_2.dat");
			} catch (FileNotFoundException e) {
				System.out.println("Error, routes file don't existe");
				System.out.println(e.getMessage());
			} catch (IOException e) {
				System.out.println("Error, can't read routes file");
				System.out.println(e.getMessage());
			}

		}

		/*
		 **************************************************************************************
		 * Get the time for the hourOfDay's API parameter
		 **************************************************************************************
		 */
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
		/*
		 **************************************************************************************
		 * numHours API parameter [1,24]
		 **************************************************************************************
		 */
		if (numHours < 1 || numHours > 24) {
			numHours = 24;

		}

		/*
		 **************************************************************************************
		 * call to the API and insert into mongoDB
		 **************************************************************************************
		 */

		Integer dates_size = dates.size();
		for (int j = 0 ; j < dates_size ; j++) {
			date = dates.get(j);
			year = Integer.parseInt(date.split("-")[0]);
			month = Integer.parseInt(date.split("-")[1]);
			day = Integer.parseInt(date.split("-")[2]);

			if (departureAirport.equals("") || arrivalAirport.equals("")) {

				// route = departureAirport;arrivalAirport
				for(int i = 0 ; i < routes_size ; i++) {

					route = route_list.get(i);
					departureAirport_aux = route.split(";")[0];
					arrivalAirport_aux   = route.split(";")[1];

					_id = departureAirport_aux+"-"+ arrivalAirport_aux+"-" + year+"-" + month+"-" + day+"-" + hourOfDay;
					if (!API.exist(_id, collection)) {

						try {
						    request = API.GET(appId,appKey,departureAirport_aux,arrivalAirport_aux,year,month,day,hourOfDay,numHours);
							JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();
							//JsonObject jsonObject = new JsonParser().parse(new FileReader("C:/Users/adilazh1/Downloads/route.json")).getAsJsonObject();
							Document doc = Document.parse(jsonObject.toString());
							doc.put("_id", _id);

							to_insert = API.toInsert(doc);
							Document doc_out = new Document();
							if (to_insert) {

								if (reformat.equals("Y")) {
									doc_out = API.reformat(doc);
								} else {
									doc_out = doc;
								}

								try {
									collection.insertOne(doc_out);
								} catch (Exception e) {
									System.out.println(e.getMessage());
								}
							}else {
								routeIgnored = departureAirport_aux + ";" + arrivalAirport_aux +"\n";
								b.write(routeIgnored);
							}

						} catch (MalformedURLException e) {
							 
							System.out.println("Error, API URL is wrong");
							System.out.println(e.getMessage()); 
							
						} catch (IOException e) {
							
							System.out.println("Error, can't open conection with API");
							System.out.println(e.getMessage());
							
						}

					}
					
				}
				log.info("routeStatus >> inserted data information : " + year + "-" + month + "-" + day + "-"+ hourOfDay);

			} else {
				
				_id = departureAirport+"-"+ arrivalAirport+"-" + year+"-" + month+"-" + day+"-" + hourOfDay;
				if (!API.exist(_id, collection)) {

					try {
						request = API.GET(appId,appKey,departureAirport,arrivalAirport,year,month,day,hourOfDay,numHours);

						JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();
						//JsonObject jsonObject = new JsonParser().parse(new FileReader("C:/Users/adilazh1/Downloads/route.json")).getAsJsonObject();
						Document doc = Document.parse(jsonObject.toString());
						doc.put("_id", _id);

						to_insert = API.toInsert(doc);
						Document doc_out = new Document();
						if (to_insert) {

							if (reformat.equals("Y")) {
								doc_out = API.reformat(doc);
							} else {
								doc_out = doc;
							}

							try {
								collection.insertOne(doc_out);
							} catch (Exception e) {
								System.out.println(e.getMessage());
							}

							log.info("routeStatus >> inserted data information : " + year + "-" + month + "-" + day
									+ "-" + hourOfDay);
						}else {
							routeIgnored = departureAirport + ";" + arrivalAirport +"\n";
							b.write(routeIgnored);
						}

					} catch (MalformedURLException e) {
						
						System.out.println("Error, API URL is wrong");
						System.out.println(e.getMessage()); 
						
					} catch (IOException e) {
						
						System.out.println("Error, can't open conection with API");
						System.out.println(e.getMessage());
						
					}
				}
			}

		}

		client.close();
		b.close();
		long finishTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time = " + finishTime);
		log.info("routeStatus >> execution finished OK, total time : " + finishTime + " ms");

	}

}
