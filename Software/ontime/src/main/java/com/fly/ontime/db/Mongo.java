package com.fly.ontime.db;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fly.ontime.load.Loader;
import com.fly.ontime.model.Rating;
import com.fly.ontime.model.UserData;
import com.fly.ontime.model.Weather;
import com.fly.ontime.util.AppParams;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;




public class Mongo 
{
	private static Logger _logger = Logger.getLogger(Mongo.class);

	private static Mongo _instance = null;
	private MongoClient client = null;
	private MongoDatabase dbOntime= null;
	private MongoDatabase dbHistoricalData = null;
	private MongoCollection<Document> airportsCollection = null;
	private MongoCollection<Document> airlinesCollection = null;
	private MongoCollection<Document> routesCollection = null;
	private MongoCollection<Document> routes2Collection = null;
	private MongoCollection<Document> weatherCollection = null;
	private MongoCollection<Document> ratingsCollection = null;

			
			
			
	private Mongo() 
	{
		init();
	}


	public static Mongo getInstance()
	{
		if (_instance == null)
			_instance = new Mongo();
		return _instance;
	}
	
	private void init()
	{
		this.client = new MongoClient();
		this.dbOntime= client.getDatabase("ontime");
		this.dbHistoricalData = client.getDatabase("historicalData");
		this.airportsCollection = dbOntime.getCollection("airports");
		this.airlinesCollection = dbOntime.getCollection("airlines");
		this.routesCollection = dbOntime.getCollection("routes");
		this.routes2Collection = dbOntime.getCollection("routes2");
		this.weatherCollection = dbHistoricalData.getCollection("airportWeather");
		this.ratingsCollection = dbHistoricalData.getCollection("routeRatings");
	}
	
	public void closeMongoClient() 
	{
		this.client.close();	
	}
	
	public void insertAirports(String airportsJson) throws Throwable 
	{
//		MongoClient client = new MongoClient();
//		//crea la db si no existe
//		MongoDatabase database = client.getDatabase("ontime");
//		//crea la collection si no existe
//		MongoCollection<Document> airportsCollection = database.getCollection("airports");
		
		//limpiamos previamente documentos (siempre actualiza)
		airportsCollection.drop();
		
		JsonObject allAirports = Json.createReader(new StringReader(airportsJson)).readObject();
		allAirports.getJsonArray("airports").forEach(t -> {
            JsonObject airport = t.asJsonObject();
            _logger.debug("airport [" + airport + "]");
    		Document doc = Document.parse(airport.toString());
    		airportsCollection.insertOne(doc);
        });
//		client.close();		
	}	
	
	public void insertAirlines(String airlinesJson) throws Throwable 
	{
		//limpiamos previamente documentos 
		airlinesCollection.drop();
		
		JsonObject allAirlines = Json.createReader(new StringReader(airlinesJson)).readObject();
        allAirlines.getJsonArray("airlines").forEach(t -> {
            JsonObject airline = t.asJsonObject();
            _logger.debug("airline [" + airline + "]");
    		Document doc = Document.parse(airline.toString());
    		airlinesCollection.insertOne(doc);
        });
	}
	
	public void insertRoutes(String routesJson) throws Throwable 
	{
//		//limpiamos previamente documentos 
//		routesCollection.drop();

		JsonObject allRoutes = Json.createReader(new StringReader(routesJson)).readObject();
		allRoutes.getJsonArray("routes").forEach(r -> {
            JsonObject route = r.asJsonObject();
            _logger.debug("route [" + route + "]");
    		Document doc = Document.parse(route.toString());
    		routesCollection.insertOne(doc);
        });
	}
	
	public void insertRoutes2(String routesJson) throws Throwable 
	{
		//limpiamos previamente documentos 
		routes2Collection.drop();

		JsonObject allRoutes = Json.createReader(new StringReader(routesJson)).readObject();
		allRoutes.getJsonArray("routes").forEach(r -> {
            JsonObject route = r.asJsonObject();
            _logger.debug("route [" + route + "]");
    		Document doc = Document.parse(route.toString());
    		routes2Collection.insertOne(doc);
        });
	}
	
	
	/**
	 * Verificacion si hay clima o no
	 * @param airport
	 * @param tsDeparture
	 * @return
	 */
	public boolean existsWeather(String airport, long tsDeparture)  
	{
		FindIterable<Document> lDoc = weatherCollection.find(Filters.and(Filters.eq("airport", airport), Filters.gte("dt_1", tsDeparture)));
		Document doc = lDoc.first();
		return (doc != null);
	}
		
	/**
	 * 
	 * @param airport
	 * @param tsDeparture 
	 * @return
	 */
	public Weather getWeather(String airport, long tsDeparture)  
	{
//		_logger.info("********airport [" + airport + "]");
		Weather weather = new Weather();
		long INTERVAL_CAPTURE = (60*60*3)/2;//3h en segundos
		FindIterable<Document> lDoc = weatherCollection.find(Filters.and(Filters.eq("airport", airport), Filters.gte("dt_1", tsDeparture)));
		Document doc = lDoc.first();
		if (doc == null) 
			return weather;
		
		List<Document> captures = new ArrayList<Document>();
		captures = (ArrayList<Document>) doc.get("list");
		for (Document capture : captures) {
			//dt esta en segundos
			long dt = new Long(capture.getInteger("dt_local")).longValue();
			//60*60*3 = 3 horas...
			if (Math.abs(dt - tsDeparture) < INTERVAL_CAPTURE) {
				weather = new Weather(
						Double.parseDouble(((Document)capture.get("main")).get("temp").toString()),
						Double.parseDouble(((Document)capture.get("main")).get("humidity").toString()),
						Double.parseDouble(((Document)capture.get("main")).get("pressure").toString()),
						((Document)((ArrayList<Document>)capture.get("weather")).get(0)).getString("main"),
						Double.parseDouble(((Document)capture.get("wind")).get("speed").toString()),
						Double.parseDouble(((Document)capture.get("wind")).get("deg").toString())
						);
				break;
			}
		}
		return weather;
	}
	
	
	public Rating getRating(String departureAirport, String arrivalAirport, String airlineFsCode, String flightNumber)  
	{
		Rating rating = new Rating();
		String key = departureAirport + "-" + arrivalAirport + "-" + airlineFsCode + "-" + flightNumber;
		_logger.info("getRating. key [" + key + "]");
		Document doc = ratingsCollection.find(Filters.eq("_id", key)).first();
		if (doc == null) {
			_logger.info("getRating. No rating for key [" + key + "]");
			return rating;
		}
		
		rating = new Rating (
				doc.getInteger("ontime"),
				doc.getInteger("late15"),
				doc.getInteger("late30"),
				doc.getInteger("late45"),
				Double.parseDouble(doc.get("delayMean").toString()),
				Double.parseDouble(doc.get("delayStandardDeviation").toString()),
				Double.parseDouble(doc.get("delayMin").toString()),
				Double.parseDouble(doc.get("delayMax").toString()),
				Double.parseDouble(doc.get("allOntimeCumulative").toString()),
				Double.parseDouble(doc.get("allOntimeStars").toString()),
				Double.parseDouble(doc.get("allDelayCumulative").toString()),
				Double.parseDouble(doc.get("allDelayStars").toString())
				);
		
		return rating;
	}

}
