package com.fly.ontime.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fly.ontime.data.api.APITypes;
import com.fly.ontime.data.api.ParamBuilder;
import com.fly.ontime.model.Route;
import com.fly.ontime.util.AppParams;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;




public class CSVUtil 
{
	private static Logger _logger = Logger.getLogger(CSVUtil.class);

	private static CSVUtil _instance = null;

	private CSVUtil() 
	{
		init();
	}


	public static CSVUtil getInstance()
	{
		if (_instance == null)
			_instance = new CSVUtil();
		return _instance;
	}

	private void init()
	{
	}


	public void toCSV(List<Route> lRoutes, String filename) throws Throwable 
	{
		try (
				BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename));
				CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(Route.getCSVHeader()));
				) {
			for (Route r : lRoutes) {
				try {
					csvPrinter.printRecord(r.toCSVEntry());
				} catch (Throwable e) {
					_logger.error("Error generating Connections CSV", e);
				}
			}
			csvPrinter.flush();            
		}
	}


}
