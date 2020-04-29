package OnTime.airportWeather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import org.bson.Document;


public class API {
	/**
	 * Method to call openweathermap API, airportWeather
	 * @param appKey
	 * @param latitude
	 * @param longitude
	 * @return response
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static String GET(String appKey, String latitude, String longitude) throws MalformedURLException, IOException {
		
		String request = "";
		String URL     = "";
		
		URL="http://api.openweathermap.org/data/2.5/forecast?lat="+latitude+"&lon="+longitude+"&appid="+appKey;
		
    	URL obj = new URL(URL);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		request = response.toString();
		return request;
		
	}
	
	/**
	 * Method to check if a document have weather data
	 * @param doc_in
	 * @return boolean
	 */
	public static boolean toInsert(Document doc_in) {
	
		ArrayList<Document> metar = (ArrayList<Document>) doc_in.get("list");
		if(metar.size()<1) {
			return false;
		}else {
			return true;
		}
	}
	/**
	 * Method to reformat weather json, for the propose of the app and performance analysis, use reformat2 method
	 * @deprecated
	 * @param doc_in
	 * @return doc_out
	 */
	public static Document reformat(Document doc_in) {
		Document out = new Document();
		
		out = doc_in;
		
		ArrayList<Document> list = (ArrayList<Document>) out.get("list");
		
		int list_size = list.size();
		String dt_txt_0 = list.get(0).getString("dt_txt");
		Integer dt_0 = list.get(0).getInteger("dt");
		String dt_txt_1 = list.get(list_size - 1).getString("dt_txt");
		Integer dt_1 = list.get(list_size - 1).getInteger("dt");
		
		out.put("dt_txt_0", dt_txt_0);
		out.put("dt_0", dt_0);
		out.put("dt_txt_1", dt_txt_1);
		out.put("dt_1", dt_1);
		
		
		return out;
	}
	
	/**
	 * Method to reorganize weather json to give i document more properly with or
	 * propose and more efficient access to the information in mongoDB architecture.
	 * 
	 * @param doc_in
	 * @param _id
	 * @param airport
	 * @return Document
	 */
	
	public static Document reformat2(Document doc_in,String _id,String airport) {
		Document out = new Document();
		
			ArrayList<Document> list = (ArrayList<Document>) doc_in.get("list");
			ArrayList<Document> lists = new ArrayList<Document>();
			Iterator<Document> list_it = list.iterator();
			Document city = (Document) doc_in.get("city");
			Integer timezone = city.getInteger("timezone");
			
			while(list_it.hasNext()) {
				Document aux = new Document();
				aux = list_it.next();
				//System.out.println("list ="+aux.toJson());
				Integer dt = (Integer) aux.get("dt");
				//System.out.println("dt="+dt);
				int dt_local = dt + timezone;
				aux.put("dt_local", dt_local);
				lists.add(aux);
			}
			
			out.put("_id", _id);
			out.put("city", city);
			out.put("list", lists);
			out.put("airport", airport);
			int list_size = lists.size();
			Integer dt_0 = lists.get(0).getInteger("dt_local");
			Integer dt_1 = lists.get(list_size - 1).getInteger("dt_local");
			Integer dt_0_utc = lists.get(0).getInteger("dt");
			Integer dt_1_utc = lists.get(list_size - 1).getInteger("dt");
			out.put("dt_0", dt_0);
			out.put("dt_1", dt_1);
			out.put("dt_0_utc", dt_0_utc);
			out.put("dt_1_utc", dt_1_utc);
			
		return out;
	}
}
