package com.fly.ontime;

import com.fly.ontime.python.PyGateway;

public class MainPython {

	public static void main(String[] args) throws Throwable {
		
		if (args[0].equals("test1")) {
			PyGateway.getInstance().test1(args[1].toUpperCase());
		} else if (args[0].equals("test2")) { 
			PyGateway.getInstance().test2(args[1].toUpperCase());
		} else if (args[0].equals("prices")) { 
			PyGateway.getInstance().getPrices(args[1].toUpperCase(), args[2].toUpperCase(), args[3]);
		} else if (args[0].equals("join")) { 
			PyGateway.getInstance().getPricesWaiting(args[1].toUpperCase(), args[2].toUpperCase(), args[3]);
		}
	}
	
}
