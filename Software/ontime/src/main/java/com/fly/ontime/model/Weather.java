package com.fly.ontime.model;

import com.fly.ontime.util.AppParams;

public class Weather {

	
	private Double temp;
	private Double humidity;
	private Double pressure;
	private String weather;//'Clear','Clouds'...
	private Double windSpeed;
	//dirección del viento
	private Double windDeg;

	
//	los campos son estos ? main.temp, main.pressure weather.main (categórica) y  wind.speed ? (del nodo que toque)
	
	
	public Weather() {
	}



	public Weather(Double temp, Double humidity, Double pressure, String weather, Double windSpeed, Double windDeg) {
		this.temp = temp;
		this.humidity = humidity;
		this.pressure = pressure;
		this.weather = weather;
		this.windSpeed = windSpeed;
		this.windDeg = windDeg;
	}



	public static String getCSVHeaderV1() {
		return 
				"temp_v1" + AppParams.get_csvSeparator() +
				"humidity_v1" + AppParams.get_csvSeparator() +
				"pressure_v1" + AppParams.get_csvSeparator() +
				"weather_v1" + AppParams.get_csvSeparator() +
				"windSpeed_v1" + AppParams.get_csvSeparator() +
				"windDeg_v1";
	}

	public static String getCSVHeaderV2() {
		return 
			"temp_v2" + AppParams.get_csvSeparator() +
			"humidity_v2" + AppParams.get_csvSeparator() +
			"pressure_v2" + AppParams.get_csvSeparator() +
			"weather_v2" + AppParams.get_csvSeparator() +
			"windSpeed_v2" + AppParams.get_csvSeparator() +
			"windDeg_v2";
	}

	public String toCSVEntry() {
		String res =  
				this.temp + AppParams.get_csvSeparator() +
				this.humidity + AppParams.get_csvSeparator() +
				this.pressure + AppParams.get_csvSeparator() +
				this.weather + AppParams.get_csvSeparator() +
				this.windSpeed + AppParams.get_csvSeparator() +
				this.windDeg;
		return res;
	}
	
	public Double getTemp() {
		return temp;
	}

	public void setTemp(Double temp) {
		this.temp = temp;
	}

	public Double getPressure() {
		return pressure;
	}

	public void setPressure(Double pressure) {
		this.pressure = pressure;
	}

	public String getWeather() {
		return weather;
	}

	public void setWeather(String weather) {
		this.weather = weather;
	}

	public Double getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(Double windSpeed) {
		this.windSpeed = windSpeed;
	}

	
	public Double getHumidity() {
		return humidity;
	}



	public void setHumidity(Double humidity) {
		this.humidity = humidity;
	}



	public Double getWindDeg() {
		return windDeg;
	}



	public void setWindDeg(Double windDeg) {
		this.windDeg = windDeg;
	}



	@Override
	public String toString() {
		return "Weather [temp=" + temp + ", humidity=" + humidity + ", pressure=" + pressure + ", weather=" + weather
				+ ", windSpeed=" + windSpeed + ", windDeg=" + windDeg + "]";
	}



	
}
