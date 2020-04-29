package OnTime.airportWeather;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author OnTime Main Class airporWteather program. This APP call Weather API
 *         to give airports weather and save it in to MongoDB. A properties file
 *         is necessary.
 */
public class Main {

	// technical variable to avoid API blocking, API's test version only give 60
	// calls/second
	private static int API_MAX = 59;

	public static void main(String[] args)
			throws JsonIOException, JsonSyntaxException, IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		/*
		 * First, read properties file, and initialize variables
		 */
		String appKey = "";
		String latitude = "";
		String longitude = "";
		ArrayList<String> IATA_list = new ArrayList<String>();
		String airport = "";

		Logger log = LogManager.getRootLogger();

		// Auxiliary variables
		String request = "";
		String _id = "";
		boolean to_insert = false;
		Calendar calendar = Calendar.getInstance();
		Integer year = calendar.get(Calendar.YEAR);
		Integer month = calendar.get(Calendar.MONTH) + 1;
		Integer day = calendar.get(Calendar.DAY_OF_MONTH);
		Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
		Document doc_out = new Document();
		String line = "";
		String data_path = "";
		String DBdatabse = "";
		String DBcollection = "";

		log.info("airportWeather >> Application started normally");

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));

			appKey = properties.getProperty("appKey");
			latitude = properties.getProperty("latitude");
			longitude = properties.getProperty("longitude");
			airport = properties.getProperty("airport");

			data_path = properties.getProperty("data_path");
			
			DBdatabse = properties.getProperty("database");
			DBcollection = properties.getProperty("collection");

		} catch (FileNotFoundException e) {
			System.out.println("Error, propeties file don't existe");
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println("Error, can't read properties file");
			System.out.println(e.getMessage());
		}

		// MongoDB client
		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase(DBdatabse);
		MongoCollection<Document> collection = database.getCollection(DBcollection);
		
		/*
		 * *****************************************************************************
		 * * If parameter airports is empty, load airports's IATA codes from input file
		 * *****************************************************************************
		 */

		if (latitude.equals("") || longitude.equals("")) {

			try {
				IATA_list = LoadAirportsIATA.get(data_path);
			} catch (FileNotFoundException e) {
				System.out.println("Error, airportss file don't existe");
				System.out.println(e.getMessage());
			} catch (IOException e) {
				System.out.println("Error, can't read airportss file");
				System.out.println(e.getMessage());
			}
		}
		if (latitude.equals("") || longitude.equals("")) {

			Iterator<String> airportCode = IATA_list.iterator();
			int i = 0;
			while (airportCode.hasNext()) {
				i++;
				line = airportCode.next();
				airport = line.split(";")[0];
				latitude = line.split(";")[1];
				longitude = line.split(";")[2];

				try {
					request = API.GET(appKey, latitude, longitude);
					_id = airport + "-" + year + "-" + month + "-" + day + "-" + hour;
					JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();

					Document doc = Document.parse(jsonObject.toString());

					to_insert = API.toInsert(doc);
					if (to_insert) {

						doc_out = doc;
						doc_out = API.reformat2(doc, _id, airport);
						
						try {
							// System.out.println(doc_out.toJson());
							collection.insertOne(doc_out);
						} catch (Exception e) {
							System.out.println(e.getMessage());
						}
					}

				} catch (MalformedURLException e) {
					System.out.println("Error, API URL is wrong");
					System.out.println(e.getMessage());
				} catch (IOException e) {
					System.out.println("Error, can't open conection with API");
					System.out.println(e.getMessage());
				}

				if (i > API_MAX) {
					i = 0;
					System.out.println(System.currentTimeMillis());
					Thread.sleep(61000);
				}

			}
			
			log.info("airportWeather >> inserted data information : " + year + "-" + month + "-" + day + "-" + hour);

		} else {
			try {

				if (airport.equals("")) {
					log.error("airportWeather >> Error, the airport parametr is empty, check file's parametrs ");
				} else {
					request = API.GET(appKey, latitude, longitude);
					_id = airport + "-" + year + "-" + month + "-" + day + "-" + hour;

					JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();
					
					Document doc = Document.parse(jsonObject.toString());

					to_insert = API.toInsert(doc);
					if (to_insert) {

						doc_out = doc;
						doc_out = API.reformat2(doc, _id, airport);

						try {
							collection.insertOne(doc_out);
						} catch (Exception e) {
							System.out.println(e.getMessage());
						}

						log.info("airportWeather >> inserted data information : " + year + "-" + month + "-" + day + "-"
								+ hour);
					}

				}
			} catch (MalformedURLException e) {
				System.out.println("Error, API URL is wrong");
				System.out.println(e.getMessage());
			} catch (IOException e) {
				System.out.println("Error, can't open conection with API");
				System.out.println(e.getMessage());
			}
		}

		client.close();
		long finishTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time = " + finishTime);
		log.info("airportWeather >> execution finished OK, total time : " + finishTime + " ms");

	}
}
