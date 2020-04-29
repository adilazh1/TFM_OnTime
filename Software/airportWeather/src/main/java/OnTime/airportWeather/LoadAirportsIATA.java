package OnTime.airportWeather;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.bson.Document;

import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class LoadAirportsIATA {
	/**
	 * Deprecated Method, use only if you wont download a sequential file with airports and coordinates given 
	 * a json with active airports.
	 * @throws JsonIOException
	 * @throws JsonSyntaxException
	 * @throws IOException
	 * @Deprecated
	 */
	public static void json2file() throws JsonIOException, JsonSyntaxException, IOException {
		JsonObject jsonObject = new JsonParser().parse(new FileReader("C:/Users/adilazh1/Google Drive/Echate_a_volar_TFM/resources/dataSources/openflights/definitive_data/allActiveAirports.json")).getAsJsonObject();
		String airport  = "";
		Double latitude  = null;
		Double longitude = null;
		FileWriter writer = new FileWriter("output.txt");
		
		Document json2doc = Document.parse(jsonObject.toString());
		ArrayList<Document> documents = (ArrayList<Document>) json2doc.get("airports");
		
		Iterator<Document> documents_it = documents.iterator();
		
		while(documents_it.hasNext()) {
			Document aux = documents_it.next();
		    airport = (String) aux.get("iata");
		    airport.replaceAll("\"", "");
		    if(!airport.isEmpty() && !airport.equals("")) {
		    	System.out.println(aux.keySet());
		    	//latitude = (Double) aux.get("latitude");
		    	//longitude = (Double) aux.get("longitude");
		    	String out="";
		    	out += airport+";"+aux.get("latitude")+";"+aux.get("longitude")+"\n";
		    	writer.write(out);
		    	
		    }
		}
		
	}
	/**
	 * Method to load airports IATA codes from a given path.
	 * @param Path of the file
	 * @return ArrayList with IATA airport's code
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static ArrayList<String> get(String Path) throws IOException,FileNotFoundException{
		
		ArrayList<String> airportsCode = new ArrayList<String>();
		
		String line = "";
			
		FileReader airports = new FileReader(Path);
		BufferedReader b = new BufferedReader(airports);
			
		while((line = b.readLine())!=null) {

			airportsCode.add(line);
			
		}
		b.close();
		
		return airportsCode;
		
	}
}
