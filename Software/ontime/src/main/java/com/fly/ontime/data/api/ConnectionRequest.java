package com.fly.ontime.data.api;


import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fly.ontime.comm.Comm;
import com.fly.ontime.load.Loader;
import com.fly.ontime.util.AppParams;
import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;




public class ConnectionRequest extends Request
{
	private static Logger _logger = Logger.getLogger(ConnectionRequest.class);



	@SuppressWarnings("rawtypes")
	public ConnectionRequest(int type, Map suffixes, Map parameters) throws Exception 
	{
		//url template tiene esta forma, hay que completarla
		//https://api.flightstats.com/flex/connections/rest/v2/json/firstflightin/#SRC#/to/#DST#/arriving_before/#YEAR#/#MONTH#/#DAY#/#HOUR#/#MIN#

		//@ojo aqui, clone...
		_params = parameters;
		_suffixParams = suffixes;

		switch(type) {
		case APITypes.CONNECTIONS_FIRST_FLIGHT_IN:
			_url = AppParams.get_firstflightinUrl();
			break;
		case APITypes.CONNECTIONS_FIRST_FLIGHT_OUT:
			_url = AppParams.get_firstflightoutUrl();
			break;
		default:
			throw new Exception("Connection Request Type not valid [" + type + "]");
		}

		//completar url con los parametros
		completeURL();
	}

	private void completeURL() {

		//https://api.flightstats.com/flex/connections/rest/v2/json/firstflightin/#SRC#/to/#DST#/arriving_before/#YEAR#/#MONTH#/#DAY#/#HOUR#/#MIN#
		_url = _url.replace("#SRC#", _suffixParams.get("src").toString())
				.replace("#DST#", _suffixParams.get("dst").toString())
				.replace("#YEAR#", _suffixParams.get("year").toString())
				.replace("#MONTH#", _suffixParams.get("month").toString())
				.replace("#DAY#", _suffixParams.get("day").toString())
				.replace("#HOUR#", _suffixParams.get("hour").toString())
				.replace("#MIN#", _suffixParams.get("min").toString())
				;
		_url =_url + "?" + map2StringWithGuava(_params);
	}

	/**
	 * Respuesta api 
	 */
	public String get() throws Exception {
		String response = Comm.getInstance().get(_url);
		_logger.info("API query url [" + _url + "]");
		_logger.info("API query response [" + response + "]");
		return response;
	}


	@SuppressWarnings("rawtypes")
	public static String map2StringWithGuava(Map map) {
	    return Joiner.on("&").withKeyValueSeparator("=").join(map);
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {

		Map reqParams = new ParamBuilder()
				.add("appId", AppParams.get_appId())
				.add("appKey", AppParams.get_appKey())
				.add("numHours", 6)
				.add("maxConnections", 1)
				.add("includeSurface", false)
				.add("payloadType", "passenger")
				.add("includeCodeshares", true)
				.add("includeMultipleCarriers", true)
				.add("maxResults", 25)
				.build();
		_logger.info(" [" + map2StringWithGuava(reqParams) + "]");
//		_logger.info(" [" + convertWithStream(reqParams) + "]");
		

	}
	

}
