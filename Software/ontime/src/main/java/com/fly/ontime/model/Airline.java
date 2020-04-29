package com.fly.ontime.model;


/**
 * Airline or Carrier
 * 
 * @author Raul
 *
 */
public class Airline {

	//https://developer.flightstats.com/api-docs/airlines/v1/airlineresponse
		
	//The Cirium code for the carrier, globally unique across time (String).
	private String fs;
	//The IATA code for the carrier
	private String iata;
	//The ICAO code for the carrier
	private String icao;
	//The name of the carrier
	private String name;
	//if the airline is currently active
	private Boolean active;
	
	
	public Airline(String fs, String iata, String icao, String name, Boolean active) {
		super();
		this.fs = fs;
		this.iata = iata;
		this.icao = icao;
		this.name = name;
		this.active = active;
	}
	
	public String getFs() {
		return fs;
	}
	public void setFs(String fs) {
		this.fs = fs;
	}
	public String getIata() {
		return iata;
	}
	public void setIata(String iata) {
		this.iata = iata;
	}
	public String getIcao() {
		return icao;
	}
	public void setIcao(String icao) {
		this.icao = icao;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Boolean getActive() {
		return active;
	}
	public void setActive(Boolean active) {
		this.active = active;
	}
	@Override
	public String toString() {
		return "Airline [fs=" + fs + ", iata=" + iata + ", icao=" + icao + ", name=" + name + ", active=" + active
				+ "]";
	}
	
	
}
