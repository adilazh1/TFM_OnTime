package dataset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * 
 * @author OnTime Main Method of dataset program, this program have access to
 *         MongoDB, routeStatus, routeRatings and airportWheather Information,
 *         to construct a csv input file for Machine Learning program.
 */
public class Main {

	// Time in Barcelone, add 2h to have UTC time.
	static int UTC = 0;

	public static void main(String[] args) throws FileNotFoundException, IOException {

		long startTime = System.currentTimeMillis();

		// Auxiliar variables
		String dataset_path = "";
		String timeZone = "";
		String DBdatabse = "";
		String DBcollection_routeStatus = "";
		String DBcollection_routeRatings = "";
		String DBcollection_airportWeather = "";
		// read properties file
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(args[0]));

			dataset_path = (String) properties.get("data_path");
			timeZone = (String) properties.get("timeZone");
			
			DBdatabse = properties.getProperty("database");
			DBcollection_routeStatus = properties.getProperty("collection_routeStatus");
			DBcollection_routeRatings = properties.getProperty("collection_routeRatings");
			DBcollection_airportWeather = properties.getProperty("collection_airportWeather");
			
			// By default timezone is Europe/Madrid
			if (timeZone.isEmpty()) {

				timeZone = "Europe/Madrid";

			}
			TimeZone tz = TimeZone.getTimeZone(timeZone);
			int offSet = tz.getOffset(new Date().getTime());
			UTC = offSet;

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
		MongoCollection<Document> routeStatus = database.getCollection(DBcollection_routeStatus);
		MongoCollection<Document> airportWeather = database.getCollection(DBcollection_airportWeather);
		MongoCollection<Document> routeRatings = database.getCollection(DBcollection_routeRatings);

		// Writer
		FileWriter fw = new FileWriter(dataset_path);
		BufferedWriter b = new BufferedWriter(fw);

		// header
		String out = "";
		out = header();
		b.write(out);

		// variables
		int i = 0;
		String departureAirportFsCode_v1 = "";
		String arrivalAirportFsCode_v1 = "";
		String year = "";
		String month = "";
		String day = "";
		String carrierFsCode_v1 = "";
		String flightNumber = "";
		String fsCodeFlightNumber = "";
		String departureDate = "";
		String departuredateUtc = "";
		String arrivalDate = "";
		String arrivalDateUtc = "";
		String flightType = "";
		Integer departureGateDelayMinutes = 0;
		Double temp_v1 = null;
		Double pressure = null;
		String weather_v1 = null;
		Double wind_speed = null;
		Double windDeg_v1 = null;
		Double humidity = null;
		Integer ontime = null;
		Integer late15 = null;
		Integer late30 = null;
		Integer late45 = null;
		Double delayMean = null;
		Double delayStandardDeviation = null;
		Double delayMin = null;
		Double delayMax = null;
		Double allOntimeCumulative_v1 = null;
		Double allOntimeStars_v1 = null;
		Double allDelayCumulative_v1 = null;
		Double allDelayStars_v1 = null;

		
		Document airport_Weather = new Document();
		Document route_ratings = new Document();
		ArrayList<Document> flights = new ArrayList();
		Document flight = new Document();
		Document delays = new Document();

		// all documents in the routStatus collection
		Iterator<Document> route = routeStatus.find().iterator();

		// for every route, we take all their flights to create a row with information
		// related to flight
		while (route.hasNext()) {

			Document route_aux = route.next();

			departureAirportFsCode_v1 = (String) ((Document) (route_aux.get("departureAirport"))).get("requestedCode");
			arrivalAirportFsCode_v1 = (String) ((Document) (route_aux.get("arrivalAirport"))).get("requestedCode");
			year = (String) ((Document) (route_aux.get("date"))).get("year");
			month = (String) ((Document) (route_aux.get("date"))).get("month");
			day = (String) ((Document) (route_aux.get("date"))).get("day");

			flights = (ArrayList<Document>) route_aux.get("flightStatuses");
			int flights_size = flights.size();

			for (int j = 0; j < flights_size; j++) {
				flight = flights.get(j);

				carrierFsCode_v1 = (String) flight.get("carrierFsCode");
				flightNumber = (String) flight.get("flightNumber");
				fsCodeFlightNumber = carrierFsCode_v1 + flightNumber;
				departureDate = (String) ((Document) (flight.get("departureDate"))).get("dateLocal");
				departuredateUtc = (String) ((Document) (flight.get("departureDate"))).get("dateUtc");
				arrivalDate = (String) ((Document) (flight.get("arrivalDate"))).get("dateLocal");
				arrivalDateUtc = (String) ((Document) (flight.get("arrivalDate"))).get("dateUtc");

				try {
					flightType = (String) ((Document) (flight.get("schedule"))).get("flightType");
				} catch (Exception e) {
					flightType = null;
				}

				try {
					delays = (Document) (flight.get("delays"));
				} catch (Exception e) {
					delays = null;
				}

				departureGateDelayMinutes = 0;
				if (delays != null) {
					departureGateDelayMinutes = (Integer) delays.get("departureGateDelayMinutes");
				}

				// Getting airport's weather
//				System.out.println("airopuerto="+departureAirportFsCode_v1);
//				 System.out.println("departureDate="+departureDate+ " En formatounix="+tsToSec8601(departureDate));
//				 System.out.println("departureDateUTC="+departuredateUtc + " En formatounix="+tsToSec8601(departuredateUtc.replace("Z", "")));
				airport_Weather = getAirportWeather(airportWeather, departureAirportFsCode_v1, departureDate);

				if (airport_Weather != null) {
					temp_v1 = Double.parseDouble(((Document) airport_Weather.get("main")).get("temp").toString());
					pressure = Double.parseDouble(((Document) airport_Weather.get("main")).get("pressure").toString());
					weather_v1 = ((Document) ((ArrayList<Document>) airport_Weather.get("weather")).get(0))
							.getString("main");
					wind_speed = Double.parseDouble(((Document) airport_Weather.get("wind")).get("speed").toString());
					windDeg_v1 = Double.parseDouble(((Document) airport_Weather.get("wind")).get("deg").toString());
					humidity = Double.parseDouble(((Document) airport_Weather.get("main")).get("humidity").toString());
				}

				// Getting route ratings
				route_ratings = getRouteRatings(routeRatings, departureAirportFsCode_v1, arrivalAirportFsCode_v1, carrierFsCode_v1,
						flightNumber);

				if (route_ratings != null) {
					try {
						ontime = (Integer) route_ratings.get("ontime");
						late15 = (Integer) route_ratings.get("late15");
						late30 = (Integer) route_ratings.get("late30");
						late45 = (Integer) route_ratings.get("late45");
						delayMean = Double.parseDouble(route_ratings.get("delayMean").toString());
						delayStandardDeviation = Double
								.parseDouble(route_ratings.get("delayStandardDeviation").toString());
						delayMin = Double.parseDouble(route_ratings.get("delayMin").toString());
						delayMax = Double.parseDouble(route_ratings.get("delayMax").toString());
						allOntimeCumulative_v1 = Double.parseDouble(route_ratings.get("allOntimeCumulative").toString());
					    allOntimeStars_v1 = Double.parseDouble(route_ratings.get("allOntimeStars").toString());
					    allDelayCumulative_v1 = Double.parseDouble(route_ratings.get("allDelayCumulative").toString());
					    allDelayStars_v1 = Double.parseDouble(route_ratings.get("allDelayStars").toString());
					} catch (Exception e) {
						System.out.println("Error" + e.getMessage() + route_ratings.toJson());
					}
				}

				/*
				 * Write the flight information
				 */
				out = "";
				out += departureAirportFsCode_v1 + ";";
				out += arrivalAirportFsCode_v1 + ";";
				out += year + ";";
				out += month + ";";
				out += day + ";";
				out += carrierFsCode_v1 + ";";
				out += flightNumber + ";";
				out += fsCodeFlightNumber + ";";
				out += departureDate + ";";
				out += departuredateUtc + ";";
				out += arrivalDate + ";";
				out += arrivalDateUtc + ";";
				out += flightType + ";";
				if (departureGateDelayMinutes == null) {
					departureGateDelayMinutes = 0;
				}
				out += departureGateDelayMinutes + ";";
				out += temp_v1 + ";";
				out += pressure + ";";
				out += weather_v1 + ";";
				out += wind_speed + ";";
				out += windDeg_v1 + ";";
				out += humidity + ";";
				out += ontime + ";";
				out += late15 + ";";
				out += late30 + ";";
				out += late45 + ";";
				out += delayMean + ";";
				out += delayStandardDeviation + ";";
				out += delayMin + ";";
				out += delayMax + ";";
				out += allOntimeCumulative_v1 + ";";
				out += allOntimeStars_v1 + ";";
				out += allDelayCumulative_v1 + ";";
				out += allDelayStars_v1 + ";";
				b.write(out + "\n");

			}

		}

		b.close();
		client.close();

		long finishTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time = " + finishTime);

	}

	/**
	 * @deprecated Deprecated method. It can be used to read a file and get all
	 *             lines in ArrayList
	 * 
	 * @param Path
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static ArrayList<String> get(String Path) throws IOException, FileNotFoundException {

		ArrayList<String> routes = new ArrayList<String>();

		String route = "";

		FileReader airports = new FileReader(Path);
		BufferedReader b = new BufferedReader(airports);
		// FileWriter writer = new FileWriter("output.txt");
		while ((route = b.readLine()) != null) {
			routes.add(route);
		}

		b.close();

		return routes;

	}

	/**
	 * @deprecated 
	 * 
	 * @param origen
	 * @param destino
	 * @param collection
	 * @return
	 */
	public static boolean exist(String origen, String destino, MongoCollection<Document> collection) {

		Document document = collection.find(Filters.and(Filters.eq("departureAirportFsCode.requestedCode", origen),
				Filters.eq("arrivalAirportFsCode.requestedCode", destino))).first();
		if (document == null) {
			// Document does not exist
			// System.out.println("no existe: "+origen+";"+destino);
			return false;
		} else {
			// We found the document
			// System.out.println(" existe: "+origen+";"+destino);
			return true;
		}
	}

	/**
	 * Method to create the header for the csv file
	 * 
	 * @return
	 */
	public static String header() {

		return "departureAirportFsCode_v1" + ";" + "arrivalAirportFsCode_v1" + ";" + "year" + ";" + "month" + ";" + "day" + ";"
				+ "carrierFsCode_v1" + ";" + "flightNumber" + ";" + "fsCodeFlightNumber" + ";" + "departureDate" + ";"
				+ "departuredateUtc" + ";" + "arrivalDate" + ";" + "arrivalDateUtc" + ";" + "flightType" + ";"
				+ "departureGateDelayMinutes" + ";" + "temp_v1" + ";" + "pressure" + ";" + "weather_v1" + ";"
				+ "wind_speed" + ";"+"windDeg_v1"+";" +"humidity" +";"+ "ontime" + ";" + "late15" + ";" + "late30" + ";" + "late45" + ";" + "delayMean"
				+ ";" + "delayStandardDeviation" + ";" + "delayMin" + ";" + "delayMax" + ";"+"allOntimeCumulative_v1"+
				";"+"allOntimeStars_v1"+";"+"allDelayCumulative_v1"+";"+"allDelayStars_v1" +"\n";

	}

	/**
	 * Method to give the weather of the airport.
	 * 
	 * @param airportWeather collection
	 * @param airport        code
	 * @param time           departure time LocalTime
	 * @return Document with weather information
	 */
	public static Document getAirportWeather(MongoCollection<Document> airportWeather, String airport, String time) {

		Document out = new Document();
		ArrayList<Document> weather_3h = new ArrayList();
		Integer timestamp = 0;
		timestamp = tsToSec8601(time);

		// airportWeather collection have an B-tree index by airport variable.
		out = airportWeather.find(Filters.and(Filters.eq("airport", airport), Filters.gte("dt_1", timestamp))).first();
		try {
			weather_3h = (ArrayList<Document>) out.get("list");
			Document weather_3h_i = new Document();
			int weather_3h_size = weather_3h.size();
			boolean found = false;
			int i = 0;
			Integer dt = 0;

			while (!found && i < weather_3h_size) {
				weather_3h_i = weather_3h.get(i);
				i++;
				dt = weather_3h_i.getInteger("dt_local");
				/*
				 * In production we will have access to updated data with the necessary
				 * frequency (Read the application document for more info) and therefore we will
				 * always have weather information in intervals of 3h to 15 days seen, so, given
				 * a flight document with local time x, exist a weather document with localtime
				 * y : |x-y|< 3/2 =5400 seconds
				 */
				if (Math.abs(dt - timestamp) < 5500) {
					found = true;
//					 System.out.println("found = dt_local="+dt +"departue local en unix="+timestamp + " departure time en local"+time);
					out = weather_3h_i;
				}
			}
			/*
			 * If weather data not found, put null in the csv.
			 */
			if (!found) {
				// System.out.println("not found = "+dt +" "+timestamp + " "+time);
				out = null;

			}
		} catch (Exception e) {
			out = null;
		}

		return out;
	}

	/**
	 * Method to give statistics of the flight
	 * 
	 * @param routeRatings
	 * @param departureAirportFsCode_v1
	 * @param arrivalAirportFsCode_v1
	 * @param carrierFsCode_v1
	 * @param flightNumber
	 * @return a document with ratings information
	 */
	public static Document getRouteRatings(MongoCollection<Document> routeRatings, String departureAirportFsCode_v1,
			String arrivalAirportFsCode_v1, String carrierFsCode_v1, String flightNumber) {
		Document out = new Document();
		boolean found = false;

		String _id = departureAirportFsCode_v1 + "-" + arrivalAirportFsCode_v1 + "-" + carrierFsCode_v1 + "-" + flightNumber;
		out = routeRatings.find(Filters.eq("_id", _id)).first();

		if (out != null) {
			found = true;

		}
		if (!found) {
			out = null;
		}

		return out;
	}

	/**
	 * Method to convert data time into unix time
	 * 
	 * @param timestamp yyyy-MM-dd'T'HH:mm:ss.SSS
	 * @return unix time
	 */
	public static Integer tsToSec8601(String timestamp) {
		if (timestamp == null)
			return null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			Date dt = sdf.parse(timestamp);
			long epoch = dt.getTime();
			// We are developing in BCN, so we add 2h to have UTC.
			epoch = epoch + UTC;
			// System.out.println("Off set time =" + UTC);
			epoch = epoch / 1000L;
			// Apaño cambio de hora el 2019-10-27T03:00.000
			// (Solo afecta a ls programas que miran al pasado)
			if (epoch < 1572145200) {
			epoch = epoch + 3600;
		}
			return (int) (epoch );
		} catch (ParseException e) {
			return null;
		}

	}
}


