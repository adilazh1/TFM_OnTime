package com.fly.ontime.model;

import java.text.ParseException;

import com.fly.ontime.util.AppParams;

public class Route {

	//user data
	private UserData userData;
	
	//Relaciona vuelos posibles en una conexión (considerando schedules y codeshares)
	//ej. BCNLIM_20190809_3
	private String idConnection;
	
	private Boolean directo;
	
	//datos sobre vuelo directo o primera escala 
	private Schedule escala1;
	
	//datos sobre segunda escala (si existe)
	private Schedule escala2;
	
	//datos clima primera escala 
	private Weather weather1;
	
	//datos clima segunda escala  (si existe)
	private Weather weather2;
	
	//datos ratings primera escala
	private Rating ratings1;
	
	//datos ratings segunda escala  (si existe)
	private Rating ratings2;
	
	
	public Route() {
		userData = new UserData();
		idConnection = "";
		directo = false;
		escala1 = new Schedule();
		escala2 = new Schedule();
		weather1 = new Weather();
		weather2 = new Weather();
		ratings1 = new Rating();
		ratings2 = new Rating();
	}


	public Route(UserData userData, String idConnection, Boolean directo, Schedule escala1, Schedule escala2) {
		this();//TODO acabar quitando
		this.userData = userData;
		this.idConnection = idConnection;
		this.directo = directo;
		this.escala1 = escala1;
		this.escala2 = escala2;
		//TODO weather, rating...
	}

	//copy constructor
	//TODO  ratings, weather...
	public Route(Route orig) {
		this(new UserData(orig.userData), orig.idConnection, orig.directo, new Schedule(orig.escala1), new Schedule(orig.escala2));
	}
	
	public static String getCSVHeader() {
		return 
				UserData.getCSVHeader() + AppParams.get_csvSeparator() +
				"idConnection"  + AppParams.get_csvSeparator() +
				"directo"  + AppParams.get_csvSeparator() +
				Schedule.getCSVHeaderV1() + AppParams.get_csvSeparator() +
				Weather.getCSVHeaderV1() + AppParams.get_csvSeparator() +
//				Rating.getCSVHeaderV1() + AppParams.get_csvSeparator() +
				Schedule.getCSVHeaderV2() + AppParams.get_csvSeparator() +
				Weather.getCSVHeaderV2() + AppParams.get_csvSeparator() //+
//				Rating.getCSVHeaderV2()
				;
	}
	
	
	public String toCSVEntry() throws ParseException {
		String res= 
			userData.toCSVEntry() + AppParams.get_csvSeparator() +
			idConnection + AppParams.get_csvSeparator() +
			directo + AppParams.get_csvSeparator() +
			escala1.toCSVEntry() + AppParams.get_csvSeparator() +
			weather1.toCSVEntry() + AppParams.get_csvSeparator()
//			ratings1.toCSVEntry() + AppParams.get_csvSeparator()
			;
//		if (!directo) {
			res += escala2.toCSVEntry() + AppParams.get_csvSeparator() +
					weather2.toCSVEntry() + AppParams.get_csvSeparator()// +
//					ratings2.toCSVEntry() + AppParams.get_csvSeparator()
					;
//		}
		return res;
	}


	public UserData getUserData() {
		return userData;
	}


	public void setUserData(UserData userData) {
		this.userData = userData;
	}


	public String getIdConnection() {
		return idConnection;
	}


	public void setIdConnection(String idConnection) {
		this.idConnection = idConnection;
	}


	public Boolean getDirecto() {
		return directo;
	}


	public void setDirecto(Boolean directo) {
		this.directo = directo;
	}


	public Schedule getEscala1() {
		return escala1;
	}


	public void setEscala1(Schedule escala1) {
		this.escala1 = escala1;
	}


	public Schedule getEscala2() {
		return escala2;
	}


	public void setEscala2(Schedule escala2) {
		this.escala2 = escala2;
	}


	public Weather getWeather1() {
		return weather1;
	}


	public void setWeather1(Weather weather1) {
		this.weather1 = weather1;
	}


	public Weather getWeather2() {
		return weather2;
	}


	public void setWeather2(Weather weather2) {
		this.weather2 = weather2;
	}


	public Rating getRatings1() {
		return ratings1;
	}


	public void setRatings1(Rating ratings1) {
		this.ratings1 = ratings1;
	}


	public Rating getRatings2() {
		return ratings2;
	}


	public void setRatings2(Rating ratings2) {
		this.ratings2 = ratings2;
	}


	@Override
	public String toString() {
		return "Route [userData=" + userData + ", idConnection=" + idConnection + ", directo=" + directo + ", escala1="
				+ escala1 + ", escala2=" + escala2 + ", weather1=" + weather1 + ", weather2=" + weather2 + ", ratings1="
				+ ratings1 + ", ratings2=" + ratings2 + "]";
	}
	
	
	
}
