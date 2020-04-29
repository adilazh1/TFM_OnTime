package com.fly.ontime.python;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.fly.ontime.util.ExecCommand;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;




public class PyGateway 
{
	private static Logger _logger = Logger.getLogger(PyGateway.class);

	private static PyGateway _instance = null;


	private PyGateway() 
	{
		init();
	}

	public static PyGateway getInstance()
	{
		if (_instance == null)
			_instance = new PyGateway();
		return _instance;
	}
	
	private void init()
	{
	}
	
	public void test1(String arg) throws Throwable  
	{
		_logger.info("Obtaining prize from python...");
		
		String s = null;
		//https://www.helicaltech.com/python-code-from-java/
		try {
			Process process = Runtime.getRuntime().exec("python " + AppParams.get_pythonDir() + "/test.py " + arg);   
			_logger.info("Call successful");
		}
		catch(Throwable e) {
			_logger.error("Python call error.", e);
		}
		_logger.info("Python prize obtained.");
	}
	
	public void test2(String arg) throws Throwable  
	{
		_logger.info("Obtaining prize from python...");
		try {
			
		}
		catch(Throwable e) {
			_logger.error("Python call error.", e);
		}
		_logger.info("Python prize obtained.");
	}
	
	public void getPrices(String src, String dst, String date) throws Throwable  
	{
		_logger.info("Obtaining price from python...");
		String s = null;
		try {
			Process process = Runtime.getRuntime().exec("python " + AppParams.get_pythonDir() + "/GetPrices.py " + src + " " + dst + " " + date);
			_logger.info("Call GetPrices successful");
		}
		catch(Throwable e) {
			_logger.error("Python GetPrices error.", e);
		}
	}
	
	//https://stackoverflow.com/questions/2559430/how-to-synchronize-java-code
	public void getPricesWaiting(String src, String dst, String date) //throws Throwable  
	{
		_logger.info("Obtaining prize from python WAITING ...");
		try {
			Process process = Runtime.getRuntime().exec("python " + AppParams.get_pythonDir() + "/GetPrices.py " + src + " " + dst + " " + date);
				
			// Ensure that the process completes
			process.waitFor();

			// Then examine the process exit code
			_logger.info("GetPrices exitValue [" + process.exitValue() + "]");
			if (process.exitValue() == 1) {
				_logger.error("exitValue = 1");
			}
			
			_logger.info("Call GetPrices successful");
		}
		catch(Throwable e) {
			_logger.error("Python GetPrices error.", e);
		}
	}
	
	public void joinDataWaiting(String src, String dst, String date) throws Throwable  
	{
		_logger.info("Joining data from python...");
		String s = null;
		try {
			String command = "python " + AppParams.get_pythonDir() + "/JoinFile.py " + src + " " + dst + " " + date;
			
			new ExecCommand(command);
	
			_logger.info("Call Join successful");
		}
		catch(Throwable e) {
			_logger.error("Python Join call error.", e);
		}
	}
	

	
	
}
