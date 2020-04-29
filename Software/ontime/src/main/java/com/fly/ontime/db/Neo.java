package com.fly.ontime.db;


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




public class Neo 
{
	private static Logger _logger = Logger.getLogger(Neo.class);

	private static Neo _instance = null;


	private Neo() 
	{
		init();
	}


	public static Neo getInstance()
	{
		if (_instance == null)
			_instance = new Neo();
		return _instance;
	}
	
	private void init()
	{
	}
	
	
}
