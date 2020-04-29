package OnTime.routeRatings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import org.bson.Document;

public class API {
	/**
	 * Method to call flightStats API, Route status (departures)
	 * 
	 * @param appId
	 * @param appKey
	 * @param departureAirport
	 * @param arrivalAirport
	 * @return response
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static String GET(String appId, String appKey, String departureAirport, String arrivalAirport)
			throws MalformedURLException, IOException {

		String request = "";
		String URL = "";
		URL = "https://api.flightstats.com/flex/ratings/rest/v1/json/route/" + departureAirport + "/" + arrivalAirport
				+ "?appId=" + appId + "&appKey=" + appKey;

		URL obj = new URL(URL);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		request = response.toString();
		return request;

	}

	/**
	 * Method to reformat json, and save only needed data
	 * 
	 * @param doc_in
	 * @return doc_out
	 */
	public static ArrayList<Document> reformat(Document doc_in, String reformat2, String departureAirport,
			String arrivalAirport, String id) {

		ArrayList<Document> docs_out = new ArrayList<Document>();
		doc_in.remove("appendix");
		String _id = "";
		String airlineFsCode = "";
		String flightNumber = "";

		if (reformat2.equals("Y")) {
			ArrayList<Document> ratings = new ArrayList<Document>();
			Document flight = new Document();
			ratings = (ArrayList<Document>) doc_in.get("ratings");
			Iterator<Document> ratings_it = ratings.iterator();
			while (ratings_it.hasNext()) {
				flight = ratings_it.next();
				airlineFsCode = (String) flight.get("airlineFsCode");
				flightNumber = flight.get("flightNumber").toString();
				_id = departureAirport + "-" + arrivalAirport + "-" + airlineFsCode + "-" + flightNumber;
				flight.put("_id", _id);
				docs_out.add(flight);
			}

		} else {
			doc_in.put("_id", id);
			docs_out.add(doc_in);
		}

		return docs_out;
	}

	/**
	 * Method to check if a document have or not flight
	 * 
	 * @param doc_in
	 * @return boolean
	 */
	public static boolean toInsert(Document doc_in) {

		ArrayList<Document> ratings = (ArrayList<Document>) doc_in.get("ratings");
		if (ratings.size() < 1) {
			return false;
		} else {
			return true;
		}

	}

}
