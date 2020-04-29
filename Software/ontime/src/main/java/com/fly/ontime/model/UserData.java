package com.fly.ontime.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.fly.ontime.util.AppParams;


public class UserData {

	private String departureAirportFsCode;
	private String arrivalAirportFsCode;
	private String departureDate;

	private Integer wPrecio;
	private Integer wRetardo;
	private Integer wDuracion;
	

	public UserData(String departureAirportFsCode, String arrivalAirportFsCode, String departureDate, Integer wPrecio,
			Integer wRetardo, Integer wDuracion) {
		this.departureAirportFsCode = departureAirportFsCode;
		this.arrivalAirportFsCode = arrivalAirportFsCode;
		this.departureDate = departureDate;
		this.wPrecio = wPrecio;
		this.wRetardo = wRetardo;
		this.wDuracion = wDuracion;
	}

	public UserData() {
		this.departureAirportFsCode = "";
		this.arrivalAirportFsCode = "";
		this.departureDate = "";
		this.wPrecio = -1;
		this.wRetardo = -1;
		this.wDuracion = -1;
	}
	
	//copy constructor
	public UserData(UserData orig) {
		this(orig.departureAirportFsCode, orig.arrivalAirportFsCode, orig.departureDate, orig.wPrecio,
				orig.wRetardo, orig.wDuracion);
	}
	
	public static String getCSVHeader() {
		return 
		"departureAirportFsCode" + AppParams.get_csvSeparator() +
		"arrivalAirportFsCode" + AppParams.get_csvSeparator() +
		"departureDate" + AppParams.get_csvSeparator() +
		"wPrecio" + AppParams.get_csvSeparator() +
		"wRetardo" + AppParams.get_csvSeparator() +
		"wDuracion";
	}
	
	public String toCSVEntry() {
		String res =  
				this.departureAirportFsCode + AppParams.get_csvSeparator() +
				this.arrivalAirportFsCode + AppParams.get_csvSeparator() +
				this.departureDate + AppParams.get_csvSeparator() +
				this.wPrecio + AppParams.get_csvSeparator() +
				this.wRetardo + AppParams.get_csvSeparator() +
				this.wDuracion;
		return res;
	}

	public int getDepartureDay() throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(AppParams.getFormattter_yyyyMMdd().parse(departureDate));
		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	public Calendar getCalendarPlusOneDay() throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(AppParams.getFormattter_yyyyMMdd().parse(departureDate));
		calendar.add(Calendar.DATE, 1);
		return calendar;
	}

	public int getDepartureDayMinusOneDay() throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(AppParams.getFormattter_yyyyMMdd().parse(departureDate));
		calendar.add(Calendar.DATE, -1);
		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	public int getDepartureMonth() throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(AppParams.getFormattter_yyyyMMdd().parse(departureDate));
		return calendar.get(Calendar.MONTH)+1;
	}

	public int getDepartureYear() throws ParseException {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(AppParams.getFormattter_yyyyMMdd().parse(departureDate));
		return calendar.get(Calendar.YEAR);
	}

	public String getDepartureAirportFsCode() {
		return departureAirportFsCode;
	}

	public void setDepartureAirportFsCode(String departureAirportFsCode) {
		this.departureAirportFsCode = departureAirportFsCode;
	}

	public String getArrivalAirportFsCode() {
		return arrivalAirportFsCode;
	}

	public void setArrivalAirportFsCode(String arrivalAirportFsCode) {
		this.arrivalAirportFsCode = arrivalAirportFsCode;
	}

	public String getDepartureDate() {
		return departureDate;
	}

	public void setDepartureDate(String departureDate) {
		this.departureDate = departureDate;
	}

	public Integer getwPrecio() {
		return wPrecio;
	}

	public void setwPrecio(Integer wPrecio) {
		this.wPrecio = wPrecio;
	}

	public Integer getwRetardo() {
		return wRetardo;
	}

	public void setwRetardo(Integer wRetardo) {
		this.wRetardo = wRetardo;
	}

	public Integer getwDuracion() {
		return wDuracion;
	}

	public void setwDuracion(Integer wDuracion) {
		this.wDuracion = wDuracion;
	}

	@Override
	public String toString() {
		return "UserData [departureAirportFsCode=" + departureAirportFsCode + ", arrivalAirportFsCode="
				+ arrivalAirportFsCode + ", departureDate=" + departureDate + ", wPrecio=" + wPrecio + ", wRetardo="
				+ wRetardo + ", wDuracion=" + wDuracion + "]";
	}
	
	
}
