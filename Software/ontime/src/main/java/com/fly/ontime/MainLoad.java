package com.fly.ontime;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fly.ontime.load.Loader;
import com.fly.ontime.model.UserData;
import com.fly.ontime.python.PyGateway;

public class MainLoad {

	private static Logger _logger = Logger.getLogger(MainLoad.class);

	public static void main(String[] args) throws Throwable {
		

		
//		//logs mongo no
//		Logger.getLogger("org").setLevel(Level.OFF);
		
		if (args.length < 1) {
			throw new Exception("Error. Wrong number of arguments. Usage Load [airports|airlines|routes|oneroute]");
		}
		
		if (args[0].equals("airports")) {
//			Loader.getInstance().loadAirports();
//			
//		} else if (args[0].equals("airlines")) {
//			Loader.getInstance().loadAirlines();
//			
//		} else if (args[0].equals("routes")) {
//			if (args.length != 2)
//				throw new Exception("Error routes. Wrong number of arguments. Usage [MainLoad routes]");
//			Loader.getInstance().loadRoutes();
			
		} else if (args[0].equals("oneroute")) {
			if (args.length != 6) 
				throw new Exception("Error oneroute. Wrong number of arguments. Usage [MainLoad src dst year month day]");
			Loader.getInstance().loadOneRoute(args[1].toUpperCase(), args[2].toUpperCase(), 
					Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
		} else if (args[0].equals("user")) {
			if (args.length != 7) 
				throw new Exception("Error user. Wrong number of arguments. Usage [MainLoad src dst year month day wPrize wDuration wDelay ]");
			UserData userData = new UserData(args[1].toUpperCase(), args[2].toUpperCase(), args[3], 
					Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
			
			long startTime = System.currentTimeMillis();
			Loader.getInstance().loadUserRequest(userData);
			long endTime = System.currentTimeMillis() - startTime;
			_logger.info("loadUserRequest time [" + endTime + "]");
		}

		
	}
	
}
