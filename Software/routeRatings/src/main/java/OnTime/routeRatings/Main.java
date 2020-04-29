package OnTime.routeRatings;

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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author OnTime Main method of routeRatings program, used to give relevant
 *         statistics of flights and save it into MongoDB
 */
public class Main {
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		/*
		 * First, read properties file, and initialize variables
		 */
		String appId = "";
		String appKey = "";
		String departureAirport = "";
		String arrivalAirport = "";
		String reformat = "";
		String reformat2 = "";
		ArrayList<String> route_list = new ArrayList<String>();

		// log4j
		Logger log = LogManager.getRootLogger();

		// Auxiliary variables
		String request = "";
		String _id = "";
		boolean to_insert = false;
		Calendar calendar = Calendar.getInstance();
		// calendar.add(Calendar.DATE, -1);
		Integer year = calendar.get(Calendar.YEAR);
		Integer month = calendar.get(Calendar.MONTH) + 1;
		Integer day = calendar.get(Calendar.DAY_OF_MONTH);
		Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
		String route = "";
		String data_path = "";
		String collection = "";
		String DBdatabase = "";

		log.info("routeRatings >> Application started normally");

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));

			appId = properties.getProperty("appId");
			appKey = properties.getProperty("appKey");
			departureAirport = properties.getProperty("departureAirport");
			arrivalAirport = properties.getProperty("arrivalAirport");

			reformat = properties.getProperty("reformat");
			reformat2 = properties.getProperty("reformat2");

			data_path = properties.getProperty("data_path");
			DBdatabase = properties.getProperty("database");
			collection = properties.getProperty("collection");

		} catch (FileNotFoundException e) {
			System.out.println("Error, propeties file don't existe");
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println("Error, can't read properties file");
			System.out.println(e.getMessage());
		}

		// MongoDB clinet
		MongoClient client = new MongoClient();
		MongoDatabase database = client.getDatabase(DBdatabase);
		MongoCollection<Document> MongoCollection = database.getCollection(collection);
		/*
		 * *****************************************************************************
		 * ******** If some parameter airport is empty, load routes's IATA codes
		 * *****************************************************************************
		 */

		if (departureAirport.equals("") || arrivalAirport.equals("")) {

			try {
				route_list = LoadRoutesIATA.get(data_path);
				// route_list =LoadRoutesIATA.get("src/main/resources/routes_2.dat");
			} catch (FileNotFoundException e) {
				System.out.println("Error, routes file don't existe");
				System.out.println(e.getMessage());
			} catch (IOException e) {
				System.out.println("Error, can't read routes file");
				System.out.println(e.getMessage());
			}

		}

		if (departureAirport.equals("") || arrivalAirport.equals("")) {
			// route = departureAirport;arrivalAirport
			Iterator<String> routes = route_list.iterator();
			while (routes.hasNext()) {
				route = routes.next();
				departureAirport = route.split(";")[0];
				arrivalAirport = route.split(";")[1];

				try {
					request = API.GET(appId, appKey, departureAirport, arrivalAirport);
					_id = departureAirport + "-" + arrivalAirport + "-" + year + "-" + month + "-" + day + "-" + hour;
					JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();
					// JsonObject jsonObject = new JsonParser().parse(new
					// FileReader("C:/Users/adilazh1/Downloads/Ratings_API.json")).getAsJsonObject();
					Document doc = Document.parse(jsonObject.toString());

					to_insert = API.toInsert(doc);
					ArrayList<Document> docs_out = new ArrayList<Document>();
					if (to_insert) {

						if (reformat.equals("Y")) {
							docs_out = API.reformat(doc, reformat2, departureAirport, arrivalAirport, _id);
						} else {
							doc.put("_id", _id);
							docs_out.add(doc);
						}

						try {
							MongoCollection.insertMany(docs_out);
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

			}

			log.info("routeRatings >> inserted data information : " + year + "-" + month + "-" + day);

		} else {
			try {
				request = API.GET(appId, appKey, departureAirport, arrivalAirport);
				_id = departureAirport + "-" + arrivalAirport + "-" + year + "-" + month + "-" + day + "-" + hour;

				JsonObject jsonObject = new JsonParser().parse(request.toString()).getAsJsonObject();
				// JsonObject jsonObject = new JsonParser().parse(new
				// FileReader("C:/Users/adilazh1/Downloads/Ratings_API.json")).getAsJsonObject();
				Document doc = Document.parse(jsonObject.toString());

				to_insert = API.toInsert(doc);
				ArrayList<Document> docs_out = new ArrayList<Document>();
				if (to_insert) {

					if (reformat.equals("Y")) {
						docs_out = API.reformat(doc, reformat2, departureAirport, arrivalAirport, _id);
					} else {
						doc.put("_id", _id);
						docs_out.add(doc);
					}

					try {
						MongoCollection.insertMany(docs_out);
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}

					log.info("routeRatings >> inserted data information : " + year + "-" + month + "-" + day);
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
		log.info("routeRatings >> execution finished OK, total time : " + finishTime + " ms");

	}
}
