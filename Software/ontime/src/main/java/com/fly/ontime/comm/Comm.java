package com.fly.ontime.comm;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fly.ontime.util.AppParams;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;




public class Comm 
{
	private static Logger _logger = Logger.getLogger(Comm.class);

	private static Comm _instance = null;


	private Comm() 
	{
		init();
	}


	public static Comm getInstance()
	{
		if (_instance == null)
			_instance = new Comm();
		return _instance;
	}
	
	private void init()
	{
	}
	
	//HTTP GET request
	private String sendGet(String url) throws Exception {

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		//add request header
//		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		_logger.info("GET request. response code [" + responseCode + "] url [" + url + "]");

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		return response.toString();
	}
	
	/**
	 * 
	 * @param url la url completamente informada
	 * @return
	 * @throws Exception
	 */
	public String get(String url) throws Exception
	{
		String jsonResult = sendGet(url);
		return jsonResult;
	}
	
}
