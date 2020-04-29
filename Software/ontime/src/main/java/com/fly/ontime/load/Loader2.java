package com.fly.ontime.load;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonReader;

import com.fly.ontime.data.api.APITypes;
import com.fly.ontime.data.api.ConnectionAPI;
import com.fly.ontime.data.api.ParamBuilder;
import com.fly.ontime.data.api.Request;
import com.fly.ontime.db.Mongo;
import com.fly.ontime.file.Datalake;
import com.fly.ontime.util.AppParams;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;




public class Loader2 
{
	private static Logger _logger = Logger.getLogger(Loader2.class);

	private static Loader2 _instance = null;


	private Loader2() 
	{
		init();
	}

	public static Loader2 getInstance()
	{
		if (_instance == null)
			_instance = new Loader2();
		return _instance;
	}
	
	private void init()
	{
	}
	
	public void loadAirlines() throws Throwable  
	{
		_logger.info("Inserting airlines...");
		
		Path p = Paths.get(AppParams.get_flightstatsDir() + "/" + "activeAirlines.json");
	    if (Files.notExists(p))
	    	throw new Exception("Error obtaining airlines file");
		String airlinesJson = IOUtils.toString(p.toUri());
		
		//TODO _id codigo IATA ?
		
		//json se guarda tal como llega
		Mongo.getInstance().insertAirlines(airlinesJson);
		_logger.info("Airlines inserted.");
	}
	
	public void loadAirports() throws Throwable 
	{
		_logger.info("Inserting airports...");
		
		Path p = Paths.get(AppParams.get_flightstatsDir() + "/" + "allActiveAirports.json");
	    if (Files.notExists(p))
	    	throw new Exception("Error obtaining airports file");
		String airportsJson = IOUtils.toString(p.toUri());
		
		//json se guarda tal como llega
		Mongo.getInstance().insertAirports(airportsJson);
		_logger.info("Airports inserted.");
	}
	
	@SuppressWarnings("unchecked")
	public void loadUserRequest(String src, String dst, Integer year, Integer month, Integer day, 
			Integer wPrize, Integer wDuration, Integer wDelay) 
			throws Throwable
	{
		_logger.info("Loading user request...");
		
		//TODO siempre es igual ? (dependiendo si se meten más apis...)
		@SuppressWarnings("rawtypes")
		Map reqParams = new ParamBuilder()
		   .add("appId", AppParams.get_appId())
		   .add("appKey", AppParams.get_appKey())
		   .add("numHours", AppParams.get_numHours())
		   .add("maxConnections", AppParams.get_maxConnections())
		   .add("includeSurface", AppParams.get_includeSurface())
		   .add("payloadType", AppParams.get_payloadType())
		   .add("includeCodeshares", AppParams.get_includeCodeshares())
		   .add("includeMultipleCarriers", AppParams.get_includeMultipleCarriers())
		   .add("maxResults", AppParams.get_maxResults())
		   .build();
		
		@SuppressWarnings("rawtypes")
		Map suffixParams = new ParamBuilder()
		   .add("src", src)
		   .add("dst", dst)
		   .add("year", year)
		   .add("month", month)
		   .add("day", day)
		   .add("hour", AppParams.get_firstflightinHour())
		   .add("min", AppParams.get_firstflightinMin())
		   .build();
		
		//si no tenemos los datos en Datalake vamos a buscarlos
		String jsonAPIData = "";
		boolean exists = Datalake.getInstance().exists(suffixParams);
		if (exists) {
			//obtención de Datalake
			jsonAPIData = Datalake.getInstance().getContent(suffixParams);
		} else {
			//obtencion datos de la API
			Request req = ConnectionAPI.getInstance().newRequest(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, suffixParams, reqParams);
			jsonAPIData = req.get();
			Datalake.getInstance().put(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, jsonAPIData, suffixParams);
		}
		
		//TODO se pueden tener versiones de la misma ruta ? (dia ruta vs momento en que se fue a buscar)
		//TODO ver si ya está en mongo (neo4j?)

		String jsonTransformed = ConnectionAPI.getInstance().transform(jsonAPIData);
		Mongo.getInstance().insertRoutes(jsonTransformed);
		
		_logger.info("Route loaded.");
	}
	
	@SuppressWarnings("unchecked")
	public void loadOneRoute(String src, String dst, Integer year, Integer month, Integer day) throws Throwable
	{
		_logger.info("Loading route...");
		
		//TODO siempre es igual ? (dependiendo si se meten más apis...)
		@SuppressWarnings("rawtypes")
		Map reqParams = new ParamBuilder()
		   .add("appId", AppParams.get_appId())
		   .add("appKey", AppParams.get_appKey())
		   .add("numHours", AppParams.get_numHours())
		   .add("maxConnections", AppParams.get_maxConnections())
		   .add("includeSurface", AppParams.get_includeSurface())
		   .add("payloadType", AppParams.get_payloadType())
		   .add("includeCodeshares", AppParams.get_includeCodeshares())
		   .add("includeMultipleCarriers", AppParams.get_includeMultipleCarriers())
		   .add("maxResults", AppParams.get_maxResults())
		   .build();
		
		@SuppressWarnings("rawtypes")
		Map suffixParams = new ParamBuilder()
		   .add("src", src)
		   .add("dst", dst)
		   .add("year", year)
		   .add("month", month)
		   .add("day", day)
		   .add("hour", AppParams.get_firstflightinHour())
		   .add("min", AppParams.get_firstflightinMin())
		   .build();
		
		//si no tenemos los datos en Datalake vamos a buscarlos
		String jsonAPIData = "";
		boolean exists = Datalake.getInstance().exists(suffixParams);
		if (exists) {
			//obtención de Datalake
			jsonAPIData = Datalake.getInstance().getContent(suffixParams);
		} else {
			//obtencion datos de la API
			Request req = ConnectionAPI.getInstance().newRequest(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, suffixParams, reqParams);
			jsonAPIData = req.get();
			Datalake.getInstance().put(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, jsonAPIData, suffixParams);
		}
		
		//TODO se pueden tener versiones de la misma ruta ? (dia ruta vs momento en que se fue a buscar)
		//TODO ver si ya está en mongo (neo4j?)

		String jsonTransformed = ConnectionAPI.getInstance().transform(jsonAPIData);
		Mongo.getInstance().insertRoutes(jsonTransformed);
		
		_logger.info("Route loaded.");
	}
	
	@SuppressWarnings("unchecked")
	public void loadRoutes() throws Throwable
	{
		_logger.info("Loading routes...");
		
//		Path p = Paths.get(AppParams.get_firstflightinDir());
//	    if (Files.notExists(p))
//	    	throw new Exception("Error obtaining firstflightinDir file");
	    
		//TODO ejemplo de llamada
		
		@SuppressWarnings("rawtypes")
		Map reqParams = new ParamBuilder()
		   .add("appId", AppParams.get_appId())
		   .add("appKey", AppParams.get_appKey())
		   .add("numHours", AppParams.get_numHours())
		   .add("maxConnections", AppParams.get_maxConnections())
		   .add("includeSurface", AppParams.get_includeSurface())
		   .add("payloadType", AppParams.get_payloadType())
		   .add("includeCodeshares", AppParams.get_includeCodeshares())
		   .add("includeMultipleCarriers", AppParams.get_includeMultipleCarriers())
		   .add("maxResults", AppParams.get_maxResults())
		   .build();

		@SuppressWarnings("rawtypes")
		Map suffixParams = new ParamBuilder()
		   .add("src", "BCN")
		   .add("dst", "LIM")
		   .add("year", 2019)
		   .add("month", 7)
		   .add("day", 19)
		   .add("hour", AppParams.get_firstflightinHour())
		   .add("min", AppParams.get_firstflightinMin())
		   .build();
		
		//si no tenemos los datos en Datalake vamos a buscarlos
		String jsonAPIData = "";
		boolean exists = Datalake.getInstance().exists(suffixParams);
		if (!exists) {
			//obtencion datos de la API
			Request req = ConnectionAPI.getInstance().newRequest(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, suffixParams, reqParams);
			jsonAPIData = req.get();
			Datalake.getInstance().put(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, jsonAPIData, suffixParams);
		} else {
			//obtención de Datalake
			jsonAPIData = Datalake.getInstance().getContent(suffixParams);
		}
		
		//TODO ver si ya está en mongo (neo4j?)
		//TODO obtener schema json para mongo (neo4j?)

		MongoClient client = new MongoClient();
		//crea la db si no existe
		MongoDatabase database = client.getDatabase("ontime");
		//crea la collection si no existe
		MongoCollection<Document> routesCollection = database.getCollection("routes");
		

		client.close();		
		
		_logger.info("Routes loaded.");

	}
	
}
