package OnTime.routeStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class API {
	/**
	 * Method to call flightStats API, Route status (departures)
	 * @param appId
	 * @param appKey
	 * @param departureAirport
	 * @param arrivalAirport
	 * @param yaer
	 * @param month
	 * @param day
	 * @param hourOfDay
	 * @param numHour
	 * @return response
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static String GET(String appId, String appKey, String departureAirport, String arrivalAirport, Integer year, Integer month,Integer day,Integer hourOfDay,Integer numHours) throws MalformedURLException, IOException {
		
		String request = "";
		String URL     = "";
		
		URL="https://api.flightstats.com/flex/flightstatus/rest/v2/json/route/status/"+departureAirport+"/"+arrivalAirport+"/dep/"+year+"/"+month+"/"+day+"/?appId="+appId+"&appKey="+appKey+"&hourOfDay="+hourOfDay+"&utc=false&numHours="+numHours;
		
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
	 * @param doc_in
	 * @return doc_out
	 */
	public static Document reformat(Document doc_in) {
		
		Document doc_out = new Document();
		
		doc_out.put("_id", doc_in.get("_id") );
		
		Document request = (Document) doc_in.get("request");
		doc_out.put("departureAirport", request.get("departureAirport"));
		doc_out.put("arrivalAirport",request.get("arrivalAirport"));
		doc_out.put("date", request.get("date"));
		doc_out.put("hourOfDay", request.get("hourOfDay"));
		doc_out.put("numHours", request.get("numHours"));
		
		ArrayList<Document> flightStatuses = (ArrayList<Document>) doc_in.get("flightStatuses");
		ArrayList<Document> flightStatuses_out = new ArrayList<Document>();
		
		for(int i = 0 ; i < flightStatuses.size() ; i++) {
			Document flight = new Document();
			flight.put("flightId", flightStatuses.get(i).get("flightId"));
			flight.put("carrierFsCode", flightStatuses.get(i).get("carrierFsCode"));
			flight.put("flightNumber", flightStatuses.get(i).get("flightNumber"));
			flight.put("departureAirportFsCode", flightStatuses.get(i).get("departureAirportFsCode"));
			flight.put("arrivalAirportFsCode", flightStatuses.get(i).get("arrivalAirportFsCode"));
			flight.put("departureDate", flightStatuses.get(i).get("departureDate"));
			flight.put("arrivalDate", flightStatuses.get(i).get("arrivalDate"));
			flight.put("status", flightStatuses.get(i).get("status"));
			flight.put("schedule", flightStatuses.get(i).get("schedule"));
			
			Document operationalTimes = new Document();
			operationalTimes = (Document) flightStatuses.get(i).get("operationalTimes");
			operationalTimes.remove("estimatedGateDeparture");
			operationalTimes.remove("estimatedRunwayDeparture");
			operationalTimes.remove("estimatedGateArrival");
			operationalTimes.remove("estimatedRunwayArrival");
			
			flight.put("operationalTimes", operationalTimes);
			
			flight.put("delays", flightStatuses.get(i).get("delays"));
			flight.put("codeshares", flightStatuses.get(i).get("codeshares"));
			flight.put("flightDurations", flightStatuses.get(i).get("flightDurations"));
			flight.put("airportResources", flightStatuses.get(i).get("airportResources"));
			flightStatuses_out.add(flight);
		}
		
		doc_out.put("flightStatuses", flightStatuses_out);
		
		return doc_out;
	}
	
	/**
	 * Method to check if a document have or not flights
	 * @param doc_in
	 * @return boolean
	 */
	public static boolean toInsert(Document doc_in) {
	
		ArrayList<Document> flightStatuses = (ArrayList<Document>) doc_in.get("flightStatuses");
		if(flightStatuses.size()<1) {
			return false;
		}else {
			return true;
		}
		
	}
	
	/**
	 * Method to check if a document exists into base
	 * @param id
	 * @param collection
	 * @return boolean
	 */
	public static boolean exist(String id , MongoCollection<Document> collection) {
		 
		Document document = collection.find(Filters.eq("_id", id)).first();
		if (document == null) {
		    //Document does not exist
			return false;
		} else {
		    //We found the document
			return true;
		}
	}

}
