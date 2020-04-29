package OnTime.routeStatusSpark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoCollection;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;


public class Spark {
/**
 * Method to call API, reformat json and save it into mongoDB
 * @param context
 * @param appId
 * @param appKey
 * @param year
 * @param month
 * @param day
 * @param hourOfDay
 * @param reformat
 * @param path
 */
	public static void run(JavaSparkContext context,String appId, String appKey, Integer year, Integer month, Integer day, Integer hourOfDay,String reformat,String path) {

		System.out.println(path);
		JavaRDD<String> routes = context.textFile(path);

		JavaRDD<Document> docs = routes.map(f -> {

			Iterator<String> route = Arrays.asList(f.split("\n")).iterator();

			Document doc = new Document();

			
			while (route.hasNext()) {
				String aux = route.next();
				String departureAirport = aux.split(";")[0];
				String arrivalAirport = aux.split(";")[1];

				String _id = departureAirport+"-" + arrivalAirport+"-" + year+"-" + month+"-" + day+"-" + hourOfDay;
				String request = "";
				String URL = "";
				
				  URL= "https://api.flightstats.com/flex/flightstatus/rest/v2/json/route/status/"+
				  departureAirport+"/"+arrivalAirport+"/dep/"+year+"/"+month+"/"+day+"/?appId="
				  +appId+"&appKey="+appKey+"&hourOfDay="+hourOfDay+"&utc=false&numHours="+24;
				  
				  URL obj = new URL(URL); HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				  
				  con.setRequestMethod("GET"); BufferedReader in = new BufferedReader(new
				  InputStreamReader(con.getInputStream())); String inputLine; StringBuffer response = new StringBuffer();
				  
				  while ((inputLine = in.readLine()) != null) { response.append(inputLine); }
				  in.close();
				 
				//JsonObject response = new JsonParser().parse(new FileReader("C:/Users/AMIGO/Downloads/route.json")).getAsJsonObject();
				request = response.toString();
				doc = Document.parse(request);
				doc.put("_id", _id);
			}

			return doc;
		});

		JavaRDD<Document> docs_out = docs.map(f -> {
			Document doc_in = f;
			Document doc_out = new Document();

			doc_out.put("_id", doc_in.get("_id"));

			Document request = (Document) doc_in.get("request");
			doc_out.put("departureAirport", request.get("departureAirport"));
			doc_out.put("arrivalAirport", request.get("arrivalAirport"));
			doc_out.put("date", request.get("date"));
			doc_out.put("hourOfDay", request.get("hourOfDay"));
			doc_out.put("numHours", request.get("numHours"));

			ArrayList<Document> flightStatuses = (ArrayList<Document>) doc_in.get("flightStatuses");
			ArrayList<Document> flightStatuses_out = new ArrayList<Document>();

			for (int i = 0; i < flightStatuses.size(); i++) {
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
			
			if(flightStatuses.size()<1) {
				Document aux = new Document();
				aux.put("_id", "-1");
				return aux;
			}else {
				return doc_out;
			}

		}).filter(f -> !((String) f.get("_id")).equals("-1") );

/*
		docs_out.foreach(f->{
			if(f!=null) {
				System.out.println(f.toJson());
			}
			
		});
*/		
		try {
			
			if(reformat.equals("Y")) {
				MongoSpark.save(docs_out);
			}else {
				MongoSpark.save(docs);
			}
			
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
