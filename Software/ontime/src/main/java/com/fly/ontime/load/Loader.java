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
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
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
import com.fly.ontime.model.Route;
import com.fly.ontime.model.UserData;
import com.fly.ontime.util.AppParams;
import com.fly.ontime.util.CSVUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;




public class Loader 
{
	private static Logger _logger = Logger.getLogger(Loader.class);


	private static Loader _instance = null;

	//cjt del universo de aeropuertos
	private Set<String> airportsSet = null;
	//idem pero manteniendo orden carga (TODO se podría unificar)
	private List<String> airportsList = null;

	private Loader() 
	{
		init();
	}

	public static Loader getInstance()
	{
		if (_instance == null)
			_instance = new Loader();
		return _instance;
	}
	
	private void init()
	{
		//logs mongo no
		Logger.getLogger("org").setLevel(Level.OFF);
		
		//carga universo aeropuertos
		airportsSet = new HashSet<String>();
		airportsList = new ArrayList<String>();
		try (Stream<String> stream = Files.lines(Paths.get(AppParams.get_airportUniversFile()))) {
			stream.forEach(l -> {
				airportsList.add(l.trim());
				airportsSet.add(l.split("\\(")[1].replace(")", ""));
			});
		} catch (IOException e) {
			_logger.error("Error loading airports univers", e);
		}
	}
	
	public Set<String> getUniversAirportsSet() {
		return airportsSet;
	}	
	public List<String> getUniversAirportsList() {
		return airportsList;
	}
	

	//Carga rutas para la fecha indicada.
	//EL conjunto de rutas a obtener para un día abarca dos días, no uno. Por eso es necesario efectuar dos peticiones. 
	//Si hay una escala, el vuelo puede llegar al día siguiente. 
	//Por ello se indica un parámetro de si es para el siguiente día 
	@SuppressWarnings("rawtypes")
	private List<Route> loadRoutes(Set<String> airports, UserData userData, Map reqParams, Map suffixParams, boolean discardFlights, 
			String departureDateToken, String departureMinusOneDateToken) throws Throwable {
		
		//si no tenemos los datos en Datalake vamos a buscarlos
		String jsonAPIData = "";
		boolean exists = Datalake.getInstance().exists(suffixParams);
		if (exists && AppParams.get_go2Datalake4Routes()) {
//			if (exists) {
			//obtención de Datalake
			jsonAPIData = Datalake.getInstance().getContent(suffixParams);
			_logger.info("data obtained from datalake");
		} else {
			//obtencion datos de la API
			Request req = ConnectionAPI.getInstance().newRequest((Integer)suffixParams.get("apiType"), suffixParams, reqParams);
			jsonAPIData = req.get();
			Datalake.getInstance().put((Integer)suffixParams.get("apiType"), jsonAPIData, suffixParams);
			_logger.info("data obtained from api");
		}
		List<Route> lRoutes = ConnectionAPI.getInstance().getConnectionInfo(airports, userData,
				getPrefixIdConnectionToken(suffixParams), jsonAPIData, discardFlights, departureDateToken, departureMinusOneDateToken);

		return lRoutes;
	}
	
	@SuppressWarnings("unchecked")
	public void loadUserRequest(UserData userData) 
			throws Throwable
	{
		_logger.info("Loading user request...");
		
		//inicialización mongo
		Mongo.getInstance();

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
		
		//params llamada dia del vuelo
		@SuppressWarnings("rawtypes")
		Map suffixParams = new ParamBuilder()
			.add("apiType", APITypes.CONNECTIONS_FIRST_FLIGHT_OUT)
		   .add("src", userData.getDepartureAirportFsCode())
		   .add("dst", userData.getArrivalAirportFsCode())
		   .add("year", userData.getDepartureYear())
		   .add("month", userData.getDepartureMonth())
		   .add("day", userData.getDepartureDay())
		   .add("hour", AppParams.get_firstflightoutHour())
		   .add("min", AppParams.get_firstflightoutMin())
		   .build();
		
		Calendar plusOneDay = userData.getCalendarPlusOneDay();
		
		//params llamada dia siguiente al vuelo para 'cazar' vuelos que llegan al día siguiente
		@SuppressWarnings("rawtypes")
		Map suffixParams2 = new ParamBuilder()
		   .add("apiType", APITypes.CONNECTIONS_FIRST_FLIGHT_IN)
		   .add("src", userData.getDepartureAirportFsCode())
		   .add("dst", userData.getArrivalAirportFsCode())
		   .add("year", plusOneDay.get(Calendar.YEAR))
		   .add("month", plusOneDay.get(Calendar.MONTH) + 1)
		   .add("day", plusOneDay.get(Calendar.DAY_OF_MONTH))
		   .add("hour", AppParams.get_firstflightinHour())
		   .add("min", AppParams.get_firstflightinMin())
		   .build();
		
		long startTime = System.currentTimeMillis();
		//rutas de 2 días (pueden llegar al día siguiente)
		List<Route> lRoutes = loadRoutes(airportsSet, userData, reqParams, suffixParams, false, null, null);
		List<Route> lRoutes2 = loadRoutes(airportsSet, userData, reqParams, suffixParams2, true, 
				getDayToken((Integer)suffixParams2.get("day")), getDayToken2(userData.getDepartureDayMinusOneDay()));
		lRoutes.addAll(lRoutes2);
		long endTime = System.currentTimeMillis() - startTime;
		_logger.info("Load routes time [" + endTime + "]");
		
		//guardamos resultado a csv
		startTime = System.currentTimeMillis();
		String srcDstDate = getCSVName(suffixParams);
		
		CSVUtil.getInstance().toCSV(lRoutes, AppParams.get_pythonDir()+"/csv/routes/" + srcDstDate + ".csv");
		endTime = System.currentTimeMillis() - startTime;
		_logger.info("CSV creation time [" + endTime + "]");

		_logger.info("User request data loaded.");
	}
	

	//Devuelve un token con una forma concreta para identificarlo en un json
	private String getDayToken(int day) {
		return String.format("%02d", day).toString() + "T";
	}

	//Devuelve un token con una forma concreta para identificarlo en un json
	private String getDayToken2(int day) {
		return String.format("%02d", day).toString() + "T";
	}
	
	//TODO esto está en clase Datalake
	@SuppressWarnings("rawtypes")
	private String getDate(Map suffixParams) {
		return suffixParams.get("year").toString() + String.format("%02d", suffixParams.get("month")) + String.format("%02d", suffixParams.get("day")).toString();
	}	
	private String getDateCSV(Map suffixParams) {
		return suffixParams.get("year").toString() + "-" + String.format("%02d", suffixParams.get("month")) + "-" + String.format("%02d", suffixParams.get("day")).toString();
	}	
	
	@SuppressWarnings("rawtypes")
	private String getPrefixIdConnectionToken(Map suffixParams) {
		return suffixParams.get("src").toString() + suffixParams.get("dst").toString() + "_" + getDate(suffixParams) + "_";
	}
	//ej.: BCN_LIM_2019-10-01
	@SuppressWarnings("rawtypes")
	private String getCSVName(Map suffixParams) {
		return suffixParams.get("src").toString() + "_" + suffixParams.get("dst").toString() + "_" + getDateCSV(suffixParams);
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
		
		
		//////////////////////////////////////////////////////////////////////////////
		//SIEMPRE VAMOS A LA API O NO
		//si no tenemos los datos en Datalake vamos a buscarlos
		String jsonAPIData = "";
		boolean exists = Datalake.getInstance().exists(suffixParams);
		if (exists && AppParams.get_go2Datalake4Routes()) {
//		if (exists) {
//			//obtención de Datalake
			jsonAPIData = Datalake.getInstance().getContent(suffixParams);
			_logger.info("Routes loades from datalake");
		} else {
			//obtencion datos de la API
			Request req = ConnectionAPI.getInstance().newRequest(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, suffixParams, reqParams);
			jsonAPIData = req.get();
			Datalake.getInstance().put(APITypes.CONNECTIONS_FIRST_FLIGHT_IN, jsonAPIData, suffixParams);
		}


		String jsonTransformed = ConnectionAPI.getInstance().transform(jsonAPIData);
		Mongo.getInstance().insertRoutes(jsonTransformed);
		
		_logger.info("Route loaded.");
	}
	
}
