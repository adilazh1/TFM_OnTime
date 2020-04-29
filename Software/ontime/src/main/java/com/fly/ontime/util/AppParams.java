package com.fly.ontime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;



public class AppParams
{
	
	private static Logger _logger = Logger.getLogger(AppParams.class);
	
	private AppParams()	{	}
	
	public static final String XML_CONF = "confOnTime.xml";
	public static final String TAG_CONF = "config";

	
	//ISO-8601 format. 
	public static String format_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	public static SimpleDateFormat formattter_ISO8601 = new SimpleDateFormat(format_ISO8601);
	public static SimpleDateFormat formattter_yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
	public static SimpleDateFormat formattter_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	public static SimpleDateFormat formattter2_yyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");
	public static SimpleDateFormat formattter_hhmm = new SimpleDateFormat("HH:mm");
	
	
	
	private static String _csvSeparator;

	private static String _appId;
	private static String _appKey;
	
	private static String _dataLakeDir;
	private static String _flightstatsDir;
	private static String _openflightsDir;
	private static String _go2Datalake4Routes;
	
	private static String _firstflightinDir;
	private static String _firstflightinUrl;
	private static String _firstflightinHour;
	private static String _firstflightinMin;
	
	private static String _firstflightoutDir;
	private static String _firstflightoutUrl;
	private static String _firstflightoutHour;
	private static String _firstflightoutMin;
	
	private static String _cfgDir;
	
	private static String _airportUniversFile;
	
	private static String _maxConnections;
	private static String _numHours;
	private static String _maxResults;
	
	private static String _includeSurface;
	private static String _payloadType;
	private static String _includeCodeshares;
	private static String _includeMultipleCarriers;
	
	private static String _pythonDir;
	private static String _mergeCSVDir;

	private static String _tempoRoutesJobGroup;
	private static String _tempoRoutesJobName;
	private static String _tempoRoutesCron;
	
	//modelo
	private static String _modelDir;
	private static String _modelIndexDir;
	private static String _modelName;
	
	//Time Zone
	private static String _timeZone;
	
	static
	{
		_csvSeparator = ConfigBundle.getInstance().getQuery("csv-separator");

		_appId = ConfigBundle.getInstance().getQuery("appId");
		_appKey = ConfigBundle.getInstance().getQuery("appKey");
		_dataLakeDir = ConfigBundle.getInstance().getQuery("datalake-dir");
		_flightstatsDir = ConfigBundle.getInstance().getQuery("flightstats-dir");
		_openflightsDir = ConfigBundle.getInstance().getQuery("openflights-dir");
		
		_go2Datalake4Routes = ConfigBundle.getInstance().getQuery("go2Datalake4Routes");

		_firstflightinDir = ConfigBundle.getInstance().getQuery("firstflightin-dir");
		_firstflightinUrl = ConfigBundle.getInstance().getQuery("firstflightin-url");
		_firstflightinHour = ConfigBundle.getInstance().getQuery("firstflightin-hour");
		_firstflightinMin = ConfigBundle.getInstance().getQuery("firstflightin-min");
		
		_firstflightoutDir = ConfigBundle.getInstance().getQuery("firstflightout-dir");
		_firstflightoutUrl = ConfigBundle.getInstance().getQuery("firstflightout-url");
		_firstflightoutHour = ConfigBundle.getInstance().getQuery("firstflightout-hour");
		_firstflightoutMin = ConfigBundle.getInstance().getQuery("firstflightout-min");
		
		_cfgDir = ConfigBundle.getInstance().getQuery("cfg-dir");

		_airportUniversFile = ConfigBundle.getInstance().getQuery("airports-univers-file");

		_maxConnections = ConfigBundle.getInstance().getQuery("maxConnections");
		_numHours = ConfigBundle.getInstance().getQuery("numHours");
		_maxResults = ConfigBundle.getInstance().getQuery("maxResults");

		_includeSurface = ConfigBundle.getInstance().getQuery("includeSurface");
		_payloadType = ConfigBundle.getInstance().getQuery("payloadType");
		_includeCodeshares = ConfigBundle.getInstance().getQuery("includeCodeshares");
		_includeMultipleCarriers = ConfigBundle.getInstance().getQuery("includeMultipleCarriers");

		_pythonDir = ConfigBundle.getInstance().getQuery("python-dir");
		_mergeCSVDir = ConfigBundle.getInstance().getQuery("merge-csv-dir");

		_modelDir = ConfigBundle.getInstance().getQuery("model-dir");
		_modelIndexDir = ConfigBundle.getInstance().getQuery("model-index-dir");
		_modelName = ConfigBundle.getInstance().getQuery("model-name");
		
		_timeZone = ConfigBundle.getInstance().getQuery("timeZone");
		

	}
	
	private static int getDateOffset() {
		TimeZone tz = TimeZone.getTimeZone(AppParams.get_timeZone());
		int offSet = tz.getOffset(new Date().getTime());
		return offSet;
	}
	
	/**
	 * Method to convert data time into unix time
	 * 
	 * @param timestamp yyyy-MM-dd'T'HH:mm:ss.SSS
	 * @return unix time
	 * @throws ParseException 
	 */
	public static Long strDate2Epoch(String strDate, SimpleDateFormat sdf) throws ParseException {
		Date dt = sdf.parse(strDate);
		long epoch = dt.getTime();
		// We are developing in BCN, so we add 2h to have UTC.
		epoch = epoch + AppParams.getDateOffset();
		return  (epoch / 1000L);
	}
	
	
	public static boolean get_go2Datalake4Routes() {
		return Boolean.valueOf(_go2Datalake4Routes);
	}

	public static String get_timeZone() {
		return _timeZone;
	}


	public static SimpleDateFormat getFormattter_ISO8601() {
		return formattter_ISO8601;
	}

	
	public static SimpleDateFormat getFormattter_yyyyMMddHHmm() {
		return formattter_yyyyMMddHHmm;
	}


	public static SimpleDateFormat getFormattter_yyyyMMdd() {
		return formattter_yyyyMMdd;
	}


	public static SimpleDateFormat getFormattter2_yyyyMMdd() {
		return formattter2_yyyyMMdd;
	}


	public static SimpleDateFormat getFormattter_hhmm() {
		return formattter_hhmm;
	}


	public static String get_dataLakeDir() {
		return _dataLakeDir;
	}


	public static String get_flightstatsDir() {
		return _dataLakeDir + "/" + _flightstatsDir;
	}


	public static String get_openflightsDir() {
		return _dataLakeDir + "/" + _openflightsDir;
	}
	

	public static String get_cfgDir() {
		return _dataLakeDir + "/" + _cfgDir;
	}


	public static String get_airportUniversFile() {
		return get_cfgDir() + "/" + _airportUniversFile;
	}


	public static String get_maxConnections() {
		return _maxConnections;
	}


	public static String get_numHours() {
		return _numHours;
	}


	public static String get_maxResults() {
		return _maxResults;
	}


	public static String get_appId() {
		return _appId;
	}


	public static String get_appKey() {
		return _appKey;
	}


	public static String get_firstflightinUrl() {
		return _firstflightinUrl;
	}

	public static String get_firstflightinDir() {
		return get_flightstatsDir() + "/" + _firstflightinDir;
	}

	public static String get_firstflightinHour() {
		return _firstflightinHour;
	}


	public static String get_firstflightinMin() {
		return _firstflightinMin;
	}

	public static String get_firstflightoutUrl() {
		return _firstflightoutUrl;
	}

	public static String get_firstflightoutDir() {
		return get_flightstatsDir() + "/" + _firstflightoutDir;
	}

	public static String get_firstflightoutHour() {
		return _firstflightoutHour;
	}

	public static String get_firstflightoutMin() {
		return _firstflightoutMin;
	}

	public static String get_includeSurface() {
		return _includeSurface;
	}


	public static String get_payloadType() {
		return _payloadType;
	}


	public static String get_includeCodeshares() {
		return _includeCodeshares;
	}


	public static String get_includeMultipleCarriers() {
		return _includeMultipleCarriers;
	}


	public static String get_pythonDir() {
		return _pythonDir;
	}


	public static String get_csvSeparator() {
		return _csvSeparator;
	}

	public static String get_modelDir() {
		return _modelDir;
	}


	public static String get_modelIndexDir() {
		return _modelIndexDir;
	}


	public static String get_modelName() {
		return _modelName;
	}

	public static String get_mergeCSVDir() {
		return _mergeCSVDir;
	}




	


}