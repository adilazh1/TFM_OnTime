package com.fly.ontime.data.api;


import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Sets;
import org.spark_project.guava.collect.Sets.SetView;

import com.fly.ontime.db.Mongo;
import com.fly.ontime.model.KCarrierAndFlight;
import com.fly.ontime.model.Rating;
import com.fly.ontime.model.Route;
import com.fly.ontime.model.Schedule;
import com.fly.ontime.model.UserData;
import com.fly.ontime.model.Weather;
import com.fly.ontime.util.AppParams;



public class ConnectionAPI 
{
	private static Logger _logger = Logger.getLogger(ConnectionAPI.class);
	private static ConnectionAPI _instance = null;


	private ConnectionAPI() 
	{
		init();
	}

	public static ConnectionAPI getInstance()
	{
		if (_instance == null)
			_instance = new ConnectionAPI();
		return _instance;
	}

	private void init()
	{
	}

	@SuppressWarnings("rawtypes")
	public ConnectionRequest newRequest(int type, Map suffixParams, Map params) throws Exception
	{
		//@todo cambiar
		if (	type != APITypes.CONNECTIONS_FIRST_FLIGHT_IN
				&& type != APITypes.CONNECTIONS_FIRST_FLIGHT_OUT
				&& type != APITypes.CONNECTIONS_LAST_FLIGHT_IN
				&& type != APITypes.CONNECTIONS_LAST_FLIGHT_OUT
				) 
			throw new Exception("Query type not correct for ConnectionAPI [" + type + "]");

		return new ConnectionRequest(type, suffixParams, params);
	}


	//actualiza la info de los codeshares para las rutas
	private void setCodeshares2Routes(List<Route> lRoutes, Set<KCarrierAndFlight> codeshares1, Set<KCarrierAndFlight> codeshares2) {
		
		//asignamos como codeshares, los pares carrier/vuelo salvo el propio
		for (Route route : lRoutes) {
			{
				Schedule s1 = route.getEscala1();
				Set<KCarrierAndFlight> setPropio = Sets.newHashSet(new KCarrierAndFlight(s1.getCarrierFsCode(), s1.getFlightNumber()));
				SetView<KCarrierAndFlight> diffSet = Sets.difference(codeshares1, setPropio);
				s1.setCodeshares(diffSet.toString());
			}
			if (route.getDirecto())
				continue;
			{
				Schedule s2 = route.getEscala2();
				SetView<KCarrierAndFlight> diff2Set = Sets.difference(codeshares2, Sets.newHashSet(new KCarrierAndFlight(s2.getCarrierFsCode(), s2.getFlightNumber())));
				s2.setCodeshares(diff2Set.toString());
			}
		}
	}
	
	//
	private List<Route> getRoutesFromConnection(Set<String> airports, UserData userData, String connId, JsonObject conn, boolean discardFlights, 
			String departureDateToken, String departureMinusOneDateToken) 
			throws ParseException {
		
		//rutas a devolver para la conexión
		List<Route> lRoutes = new ArrayList<Route>();
		//estructura para codeshares
		Set<KCarrierAndFlight> codeshares1 = Sets.newHashSet();
		Set<KCarrierAndFlight> codeshares2 = Sets.newHashSet();
		//es primera escala o no
		boolean firstSched = true;
		
		JsonArray schedules = conn.getJsonArray("scheduledFlight");
		//directo o no
		boolean isDirect = (schedules.size() == 1);
		
		//sale al día siguiente y es directo
		if (discardFlights && isDirect) {
			_logger.info("scheduledFlight discarded (departures next day, is direct)");
			return null;
		}
		
		//schedules hasta 2 (una escala)
		for (JsonValue schedVal : schedules) {
			JsonObject sched = schedVal.asJsonObject();
			
			//----------------------------------------------------------------------------	PRIMERA ESCALA
			if (firstSched) {
				
				firstSched = false;
	        	if (departureMinusOneDateToken != null && sched.getString("departureTime").contains(departureMinusOneDateToken)) {
	            	_logger.info("scheduledFlight discarded (departures before user departure day!)");
	            	break;	
	        	}
	        	if (discardFlights && sched.getString("departureTime").contains(departureDateToken)) {
	            	_logger.info("scheduledFlight discarded (departures next day, is first schedule)");
	            	break;	
	        	}
	        	//si aeropuerto de llegada de la primera escala no está en el universo de los posibles, se descarta la entrada
	        	if (!airports.contains(sched.getString("arrivalAirportFsCode"))) {
	            	_logger.info("scheduledFlight discarded. Airport not in univers [" + sched.getString("arrivalAirportFsCode") + "]");
	            	break;	
	        	}
	        	
	        	Route routeSched = new Route();
	        	routeSched.setDirecto(isDirect);
	    		routeSched.setUserData(userData);
	    		routeSched.setIdConnection(connId);
	    		lRoutes.add(routeSched);
	        	Schedule s = new Schedule(
	        			sched.getString("departureAirportFsCode"),
	        			sched.getString("departureTime"),
	        			sched.getString("carrierFsCode"),
	        			sched.getString("flightNumber"),
	        			sched.getString("arrivalAirportFsCode"),
	        			sched.getString("arrivalTime")
	        			);
	        	routeSched.setEscala1(s);
	        	//info para codeshares
				codeshares1.add(new KCarrierAndFlight(s.getCarrierFsCode(), s.getFlightNumber()));
	        	//clima aeropuerto origen escala1
	        	Weather weather = Mongo.getInstance().getWeather(s.getDepartureAirportFsCode(), 
	        			AppParams.strDate2Epoch(s.getDepartureTime(), AppParams.getFormattter_ISO8601()));
	        	routeSched.setWeather1(weather);
	        	
	        	
	    		//posibles codeshares
	    		JsonArray csArray = sched.getJsonArray("codeshares");
	    		if (csArray != null) {
    				for (JsonValue cs : csArray) {
	    				JsonObject codeShare = cs.asJsonObject();
	    				//copiamos schedule
	    				Schedule scs = new Schedule(s);
	    				
	    				//flightNumber siempre el de codeshare
	    				scs.setFlightNumber((codeShare.get("flightNumber") == null) ? null : codeShare.getString("flightNumber"));
	    				if (codeShare.get("carrierFsCode") != null)
	    					scs.setCarrierFsCode(codeShare.getString("carrierFsCode"));
	    				
	    				//ojo flightNumber puede ser nulo, por lo que el orden en el equals es importante
	    				//si el codeshare al final no es info nueva del schedule, se considera lo mismo y se descarta lo que se deriva de él
	    				if (s.getFlightNumber().equals(scs.getFlightNumber()) && s.getCarrierFsCode().equalsIgnoreCase(scs.getCarrierFsCode())) {
	    					continue;
	    				}
	    				{
	    					Route routeCS = new Route();
	    					routeCS.setDirecto(isDirect);
	    					routeCS.setUserData(userData);
	    					routeCS.setIdConnection(connId);
	    					lRoutes.add(routeCS);
	    					routeCS.setEscala1(scs);
	    					codeshares1.add(new KCarrierAndFlight(scs.getCarrierFsCode(), scs.getFlightNumber()));
	    					//clima aeropuerto origen escala1 no cambia
	    					routeCS.setWeather1(weather);
	    					
	    				}
	    			};
	            }
			//---------------------------------------------------------------------------------------------------	SEGUNDA ESCALA
			} else {
				//TODO ver donde route.setdirecto(true)
				//lRoutes contiene las n escalas1 posibles (sean codeshare o no)
	        	Schedule s = new Schedule(
	        			sched.getString("departureAirportFsCode"),
	        			sched.getString("departureTime"),
	        			sched.getString("carrierFsCode"),
	        			sched.getString("flightNumber"),
	        			sched.getString("arrivalAirportFsCode"),
	        			sched.getString("arrivalTime")
	        			//TODO codeshare...
	        			);
	        	//clima aeropuerto origen escala2
	        	Weather weather = Mongo.getInstance().getWeather(s.getDepartureAirportFsCode(), 
	        			AppParams.strDate2Epoch(s.getDepartureTime(), AppParams.getFormattter_ISO8601()));
	        	
	        	//schedule pasa a ser la 2a escala de las rutas que ya hemos creado al tratar primer schedule
	        	lRoutes.forEach(r -> {
	        		r.setEscala2(s);//new Schedule(s));//TODO usamos el mismo para mayor eficiencia ?
	        		codeshares2.add(new KCarrierAndFlight(s.getCarrierFsCode(), s.getFlightNumber()));
	        		//mismo clima y ratings, escala 2 es la misma para estas rutas
	        		r.setWeather2(weather);
	        	});
	        	
	    		//posibles codeshares; clonamos las rutas que llevamos creadas para cada codeshare nuevo
	        	//clima no cambia, ratings sí
	    		JsonArray csArray = sched.getJsonArray("codeshares");
	    		if (csArray != null) {
		    		int SIZETEMP = lRoutes.size();
    				for (JsonValue cs : csArray) {
	    				JsonObject codeShare = cs.asJsonObject();
	    				Schedule scs = new Schedule(s);
	    				
	    				//flightNumber siempre el de codeshare
	    				scs.setFlightNumber((codeShare.get("flightNumber") == null) ? null : codeShare.getString("flightNumber"));
	    				if (codeShare.get("carrierFsCode") != null)
	    					scs.setCarrierFsCode(codeShare.getString("carrierFsCode"));
	    				
	    				//ojo flightNumber puede ser nulo, por lo que el orden en el equals es importante
	    				//si el codeshare al final no es info nueva del schedule, se considera lo mismo y se descarta lo que se deriva de él
	    				if (s.getFlightNumber().equals(scs.getFlightNumber()) && s.getCarrierFsCode().equalsIgnoreCase(scs.getCarrierFsCode())) {
	    					continue;
	    				}
	    				
	    				//creación de nuevas rutas por codeshares de segundo schedule
	    				for (int i = 0 ; i < SIZETEMP ; i++) {
	    					Route r = new Route(lRoutes.get(i));
	    					//codeshare es escala2 de las rutas existentes previamente
	    					r.setEscala2(scs);
	    					codeshares2.add(new KCarrierAndFlight(scs.getCarrierFsCode(), scs.getFlightNumber()));
	    					r.setWeather2(weather);
	    					
	    					lRoutes.add(r);
	    				}
    				}
	            }
			}
		}
		setCodeshares2Routes(lRoutes, codeshares1, codeshares2);
		return lRoutes;
	}

	/**
	 * 
	 * @param connectionsJson
	 * @param discardFlights Se descartarán vuelos que salen al día siguiente (directos o en primera escala)
	 * @return
	 * @throws Exception
	 */
	public List<Route> getConnectionInfo(Set<String> airports, UserData userData, String prefixIdConn, String connectionsJson, boolean discardFlights, 
			String departureDateToken, String departureMinusOneDateToken) 
			throws Exception {

		List<Route> lConnections = new ArrayList<Route>();
		int idDayConn = 0;

		JsonObject connections = Json.createReader(new StringReader(connectionsJson)).readObject();
		_logger.info("transform. routes from api [" + connections + "]");

		JsonArray connectionsArr = connections.getJsonArray("connections");
		for (JsonValue connVal : connectionsArr) {
			JsonObject conn = connVal.asJsonObject();
			//id connection
			String connId = prefixIdConn + (++idDayConn); 

			List<Route> connectionRoutes = getRoutesFromConnection(airports, userData, connId, conn, discardFlights, departureDateToken, departureMinusOneDateToken);
			if (connectionRoutes != null)
				lConnections.addAll(connectionRoutes);
		};
		return lConnections;
	}



	/**
	 * Transformación json de la api al schema que interesa (lista de schedules, es decir, de 
	 * vuelos independientes)
	 * 
	 * @param connectionsJson
	 * @return
	 * @throws Exception
	 */
	public String transform(String connectionsJson) throws Exception {

		JsonObject connections = Json.createReader(new StringReader(connectionsJson)).readObject();
		_logger.info("transform. routes from api [" + connections + "]");

		//lista schedules a devolver
		JsonArrayBuilder allSchedules = Json.createArrayBuilder();

		connections.getJsonArray("connections").forEach(c -> {
			JsonObject conn = c.asJsonObject();
			conn.getJsonArray("scheduledFlight").forEach(sf -> {
				JsonObject sched = sf.asJsonObject();
				JsonObject newSchedFromSF = createSched(sched, sched.get("carrierFsCode"));
				allSchedules.add(newSchedFromSF);
				//posibles codeshares
				JsonArray csArray = sched.getJsonArray("codeshares");
				if (csArray != null) {
					csArray.forEach(cs -> {
						JsonObject codeShare = cs.asJsonObject();
						JsonObject newSchedFromCS = createSched(sched, codeShare.get("carrierFsCode"));
						allSchedules.add(newSchedFromCS); 
					});
				}
			});
		});
		JsonObject res = Json.createObjectBuilder()
				.add("routes", allSchedules.build())
				.build();

		_logger.info("transform. routes json transformed [" + res + "]");
		return res.toString();
	}

	//Creación según carrier
	private JsonObject createSched(JsonObject sched, JsonValue carrier) {
		JsonObject obj = Json.createObjectBuilder()
				.add("departureAirportFsCode", sched.getString("departureAirportFsCode"))
				.add("arrivalAirportFsCode", sched.getString("arrivalAirportFsCode"))
				.add("flightNumber", sched.getString("flightNumber"))
				.add("carrierFsCode", carrier)
				.add("departureTime", sched.getString("departureTime"))
				.add("arrivalTime", sched.getString("arrivalTime"))
				.build();
		return obj;
	}

}
