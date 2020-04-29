package com.fly.ontime.view.model;

import java.time.LocalDate;

import javafx.beans.property.IntegerProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;

public class LayoutUserData {

	private final StringProperty  aeropuertoSalida;
	private final StringProperty  aeropuertoLlegada;
	private final ObjectProperty<LocalDate> fechaSalidaVuelo;
	
	//separamos en tres para mayor claridad
	private final IntegerProperty pesoPrecio;
	private final IntegerProperty pesoDuracion;
	private final IntegerProperty pesoRetraso;
	
	public LayoutUserData() {
		this("", "", 2000, 12, 1, -1, -1, -1); //TODO fecha
	}
	
	public LayoutUserData(String aeropuertoSalida, String aeropuertoLlegada,
			int ano, int mes, int dia,  
			int pesoPrecio, int pesoDuracion, int pesoRetraso) {
		
		this.aeropuertoSalida = new SimpleStringProperty(aeropuertoSalida);
		this.aeropuertoLlegada = new SimpleStringProperty(aeropuertoLlegada);
		this.fechaSalidaVuelo = new SimpleObjectProperty<LocalDate>(LocalDate.of(ano, mes, dia));
		this.pesoPrecio = new SimpleIntegerProperty(pesoPrecio);
		this.pesoDuracion = new SimpleIntegerProperty(pesoDuracion);
		this.pesoRetraso = new SimpleIntegerProperty(pesoRetraso);
	}

	public String getAeropuertoSalida() {
		return aeropuertoSalida.get();
	}

	public String getAeropuertoLlegada() {
		return aeropuertoLlegada.get();
	}

	public LocalDate getFechaSalidaVuelo() {
		return fechaSalidaVuelo.get();
	}

	public int getPesoPrecio() {
		return pesoPrecio.get();
	}

	public int getPesoDuracion() {
		return pesoDuracion.get();
	}

	public int getPesoRetraso() {
		return pesoRetraso.get();
	}

}
