package OnTime.routeStatus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.FileWriter;

public class LoadRoutesIATA {
	
	/**
	 * Method to load routes IATA codes.
	 * @param Path of the file
	 * @return ArrayList with IATA airport's code
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static ArrayList<String> get(String Path) throws IOException,FileNotFoundException{
		
		ArrayList<String> routes = new ArrayList<String>();
		
		String route = "";
			
		FileReader airports = new FileReader(Path);
		BufferedReader b = new BufferedReader(airports);
		//FileWriter writer = new FileWriter("output.txt");
		while((route = b.readLine())!=null) {			
			routes.add(route);	
		}

		b.close();
		
		return routes;
		
	}

}
