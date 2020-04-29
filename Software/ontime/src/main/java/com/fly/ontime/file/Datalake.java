package com.fly.ontime.file;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fly.ontime.data.api.APITypes;
import com.fly.ontime.data.api.ParamBuilder;
import com.fly.ontime.util.AppParams;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;




public class Datalake 
{
	private static Logger _logger = Logger.getLogger(Datalake.class);

	private static Datalake _instance = null;
//	private static String _dirBaseConnectionAPI = AppParams.get_firstflightinDir();
	
	private Datalake() 
	{
		init();
	}


	public static Datalake getInstance()
	{
		if (_instance == null)
			_instance = new Datalake();
		return _instance;
	}
	
	private void init()
	{
	}
	
	@SuppressWarnings("rawtypes")
	public String getDate(Map suffixParams) {
		return suffixParams.get("year").toString() + String.format("%02d", suffixParams.get("month")) + String.format("%02d", suffixParams.get("day")).toString();
	}
	
	@SuppressWarnings("rawtypes")
	public String getConnFileName(Map suffixParams) {
		return suffixParams.get("src").toString() + suffixParams.get("dst").toString() + ".json";
	}
	
	@SuppressWarnings({ "rawtypes", "resource" })
	//TODO apiType se usará para distinguir si se usa otra
	public void put(int apiType, String json, Map suffixParams) throws Throwable 
	{
//		String dirBase = AppParams.get_firstflightinDir();
//		String jsonName = suffixParams.get("src").toString() + suffixParams.get("dst").toString() + ".json";
//		String date = suffixParams.get("year").toString() + String.format("%02d", suffixParams.get("month")) + suffixParams.get("day").toString();
		String date = getDate(suffixParams);
		String jsonName = getConnFileName(suffixParams);
		Path dstPath = FileSystems.getDefault().getPath(getConnectionAPIPath((Integer)suffixParams.get("apiType")), date);
		try {
			if (!Files.exists(dstPath)) {
				Files.createDirectory(dstPath);
				_logger.info("Created directory [" + dstPath.toString() + "]");
			}

			//Write JSON file
			Path dstFile = FileSystems.getDefault().getPath(getConnectionAPIPath((Integer)suffixParams.get("apiType")), date, jsonName);
			FileWriter file = new FileWriter(dstFile.toFile());
			file.write(json);
			file.flush();
			
//			{
//				//ESTO SIRVE SI SE HACE ORDENADO
//				//NO NECESARIO SI SE MIRA EXISTENCIA DE JSONS
//				//save state (date, jsonName)
//				//TODO param
//				Path stateFile = FileSystems.getDefault().getPath(AppParams.get_stateDir(), "lastConnectionRequested.txt");
//				FileWriter file = new FileWriter(stateFile.toFile());
//				file.write(date + "," + jsonName);
//				file.flush();
//			}
			
		} catch (Throwable e) {
			_logger.error("Error saving Connection json [" + jsonName + "] for date [" + date + "]", e);
			throw e;
		}
	}

	@SuppressWarnings("rawtypes")
	public String getContent(Map suffixParams) throws Throwable 
	{
		String date = getDate(suffixParams);
		String jsonName = getConnFileName(suffixParams);
		Path file = FileSystems.getDefault().getPath(getConnectionAPIPath((Integer)suffixParams.get("apiType")), date, jsonName);
        String content = "";
        try {
            content = new String(Files.readAllBytes(file));
        }
        catch (Throwable e) {
			_logger.error("Error getting Connection json [" + jsonName + "] for date [" + date + "]", e);
            throw e;
        }
        return content;
	}
	
	@SuppressWarnings("rawtypes")
	public boolean exists(Map suffixParams) throws Exception 
	{
		String date = getDate(suffixParams);
		String jsonName = getConnFileName(suffixParams);
		Path file = FileSystems.getDefault().getPath(
				getConnectionAPIPath((Integer)suffixParams.get("apiType")), date, jsonName);
		return Files.exists(file);
	}
	
	//TODO en alguna factory...
	private String getConnectionAPIPath(int apiType) throws Exception 
	{
		String path;
		switch(apiType) {
		case APITypes.CONNECTIONS_FIRST_FLIGHT_IN:
			path = AppParams.get_firstflightinDir();
			break;
		case APITypes.CONNECTIONS_FIRST_FLIGHT_OUT:
			path = AppParams.get_firstflightoutDir();
			break;
		default:
			throw new Exception("Connection Request Type not valid [" + apiType + "]");
		}
		return path;
	}
	
}
