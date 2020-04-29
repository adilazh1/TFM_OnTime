package com.fly.ontime.model;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import com.fly.ontime.util.AppParams;

public class Schedule {

	
	private String departureAirportFsCode;
	private String departureTime;
	private String carrierFsCode;
	private String flightNumber;
	private String arrivalAirportFsCode;
	private String arrivalTime;
	
	private String codeshares;

	
	public Schedule() {
//		this.departureAirportFsCode = "";
//		this.departureTime = "";
//		this.carrierFsCode = "";
//		this.flightNumber = "";
//		this.arrivalAirportFsCode = "";
//		this.arrivalTime = "";
//		this.codeshares = "";
	}
	
	public Schedule(String departureAirportFsCode, String departureTime, String carrierFsCode, String flightNumber,
			String arrivalAirportFsCode, String arrivalTime//, String codeshares
			) {
		this.departureAirportFsCode = departureAirportFsCode;
		this.departureTime = departureTime;
		this.carrierFsCode = carrierFsCode;
		this.flightNumber = flightNumber;
		this.arrivalAirportFsCode = arrivalAirportFsCode;
		this.arrivalTime = arrivalTime;
//		this.codeshares = codeshares;
	}

	//copy constructor
	public Schedule(Schedule orig) {
		this(orig.departureAirportFsCode, orig.departureTime, orig.carrierFsCode, orig.flightNumber,
				orig.arrivalAirportFsCode, orig.arrivalTime);
	}


	public static String getCSVHeaderV1() {
		return 
		"departureAirportFsCode_v1" + AppParams.get_csvSeparator() +
//		"departureTime_v1" + AppParams.get_csvSeparator() +
		"fSalida_v1" + AppParams.get_csvSeparator() +
		"hSalida_v1" + AppParams.get_csvSeparator() +
		"carrierFsCode_v1" + AppParams.get_csvSeparator() +
		"flightNumber_v1" + AppParams.get_csvSeparator() +
		"arrivalAirportFsCode_v1" + AppParams.get_csvSeparator() +
//		"arrivalTime_v1" + AppParams.get_csvSeparator() +
		"fLlegada_v1" + AppParams.get_csvSeparator() +
		"hLlegada_v1" + AppParams.get_csvSeparator() +
		"codeshares_v1";
	}
	
	public static String getCSVHeaderV2() {
		return 
		"departureAirportFsCode_v2" + AppParams.get_csvSeparator() +
//		"departureTime_v2" + AppParams.get_csvSeparator() +
		"fSalida_v2" + AppParams.get_csvSeparator() +
		"hSalida_v2" + AppParams.get_csvSeparator() +
		"carrierFsCode_v2" + AppParams.get_csvSeparator() +
		"flightNumber_v2" + AppParams.get_csvSeparator() +
		"arrivalAirportFsCode_v2" + AppParams.get_csvSeparator() +
//		"arrivalTime_v2" + AppParams.get_csvSeparator() +
		"fLlegada_v2" + AppParams.get_csvSeparator() +
		"hLlegada_v2" + AppParams.get_csvSeparator() +
		"codeshares_v2";
	}
	
	public String toCSVEntry() throws ParseException {
		String res = 
				this.departureAirportFsCode + AppParams.get_csvSeparator() +
//				this.departureTime + AppParams.get_csvSeparator() +
				getDepartureDate() + AppParams.get_csvSeparator() +
				getDepartureHour() + AppParams.get_csvSeparator() +
				this.carrierFsCode + AppParams.get_csvSeparator() +
				this.flightNumber + AppParams.get_csvSeparator() +
				this.arrivalAirportFsCode + AppParams.get_csvSeparator() +
//				this.arrivalTime + AppParams.get_csvSeparator() +
				getArrivalDate() + AppParams.get_csvSeparator() +
				getArrivalHour() + AppParams.get_csvSeparator() +
				this.codeshares;
		return res;
	}
	
	public String getDepartureAirportFsCode() {
		return departureAirportFsCode;
	}

	public void setDepartureAirportFsCode(String departureAirportFsCode) {
		this.departureAirportFsCode = departureAirportFsCode;
	}

	public String getDepartureTime() {
		return departureTime;
	}

	public void setDepartureTime(String departureTime) {
		this.departureTime = departureTime;
	}
	
	public String getDepartureDate() throws ParseException  {
		return (departureTime == null) ? "null" :
			AppParams.getFormattter2_yyyyMMdd().format(AppParams.getFormattter_ISO8601().parse(departureTime));
	}
	
	public String getDepartureHour() throws ParseException  {
		return (departureTime == null) ? "null" :
			AppParams.getFormattter_hhmm().format(AppParams.getFormattter_ISO8601().parse(departureTime));
	}
	
	public String getArrivalDate() throws ParseException  {
		return (arrivalTime == null) ? "null" :
			AppParams.getFormattter2_yyyyMMdd().format(AppParams.getFormattter_ISO8601().parse(arrivalTime));
	}
	
	public String getArrivalHour() throws ParseException  {
		return (arrivalTime == null) ? "null" :
			AppParams.getFormattter_hhmm().format(AppParams.getFormattter_ISO8601().parse(arrivalTime));
	}
	

	public String getCarrierFsCode() {
		return carrierFsCode;
	}

	public void setCarrierFsCode(String carrierFsCode) {
		this.carrierFsCode = carrierFsCode;
	}

	public String getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(String flightNumber) {
		this.flightNumber = flightNumber;
	}

	public String getArrivalAirportFsCode() {
		return arrivalAirportFsCode;
	}

	public void setArrivalAirportFsCode(String arrivalAirportFsCode) {
		this.arrivalAirportFsCode = arrivalAirportFsCode;
	}

	public String getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(String arrivalTime) {
		this.arrivalTime = arrivalTime;
	}

	public String getCodeshares() {
		return codeshares;
	}

	public void setCodeshares(String codeshares) {
		this.codeshares = codeshares;
	}

	@Override
	public String toString() {
		return "Schedule [departureAirportFsCode=" + departureAirportFsCode + ", departureTime=" + departureTime
				+ ", carrierFsCode=" + carrierFsCode + ", flightNumber=" + flightNumber + ", arrivalAirportFsCode="
				+ arrivalAirportFsCode + ", arrivalTime=" + arrivalTime + ", codeshares=" + codeshares + "]";
	}

	
	
}
