package com.fly.ontime.view.model;

import java.io.Serializable;

import com.fly.ontime.view.OnTimeApp;

import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.control.Hyperlink;

public class LayoutFlight  {


	private final StringProperty  directo;

	//vuelo 1
	private final StringProperty  aeropuertoSalida1;
	private final StringProperty  fechaSalida1;
	private final StringProperty  horaSalida1;
	private final StringProperty  aerolinea1;
	private final StringProperty  numVuelo1;
	private final StringProperty  aeropuertoLlegada1;
	private final StringProperty  fechaLlegada1;
	private final StringProperty  horaLLegada1;

	//vuelo 2 (si hay)
	private final StringProperty  aeropuertoSalida2;
	private final StringProperty  fechaSalida2;
	private final StringProperty  horaSalida2;
	private final StringProperty  aerolinea2;
	private final StringProperty  numVuelo2;
	private final StringProperty  aeropuertoLlegada2;
	private final StringProperty  fechaLlegada2;
	private final StringProperty  horaLLegada2;


	private final StringProperty precio;
	private final StringProperty duracion;
	private final StringProperty delayed;//predictionTotal
//	private final StringProperty href;
	private final Hyperlink href;




	public LayoutFlight(OnTimeApp mainApp, //String aeropuertoSalida, String aeropuertoLlegada, String fechaSalidaVuelo,
			String directo, String aeropuertoSalida1, String fechaSalida1,
			String horaSalida1, String aerolinea1, String numVuelo1,
			String aeropuertoLlegada1, String fechaLlegada1, String horaLLegada1,
			String aeropuertoSalida2, String fechaSalida2, String horaSalida2,
			String aerolinea2, String numVuelo2, String aeropuertoLlegada2,
			String fechaLlegada2, String horaLlegada2, 
			String precio, String duracion,
			String href, String delayed) {
		super();
//		this.aeropuertoSalida = new SimpleStringProperty(aeropuertoSalida);
//		this.aeropuertoLlegada = new SimpleStringProperty(aeropuertoLlegada);
//		this.fechaSalidaVuelo = new SimpleStringProperty(fechaSalidaVuelo);
		this.directo = new SimpleStringProperty(directo);
		this.aeropuertoSalida1 = new SimpleStringProperty(aeropuertoSalida1);
		this.fechaSalida1 = new SimpleStringProperty(fechaSalida1);
		this.horaSalida1 = new SimpleStringProperty(horaSalida1);
		this.aerolinea1 = new SimpleStringProperty(aerolinea1);
		this.numVuelo1 = new SimpleStringProperty(numVuelo1);
		this.aeropuertoLlegada1 = new SimpleStringProperty(aeropuertoLlegada1);
		this.fechaLlegada1 = new SimpleStringProperty(fechaLlegada1);
		this.horaLLegada1 = new SimpleStringProperty(horaLLegada1);
		this.aeropuertoSalida2 = new SimpleStringProperty(aeropuertoSalida2);
		this.fechaSalida2 = new SimpleStringProperty(fechaSalida2);
		this.horaSalida2 = new SimpleStringProperty(horaSalida2);
		this.aerolinea2 = new SimpleStringProperty(aerolinea2);
		this.numVuelo2 = new SimpleStringProperty(numVuelo2);
		this.aeropuertoLlegada2 = new SimpleStringProperty(aeropuertoLlegada2);
		this.fechaLlegada2 = new SimpleStringProperty(fechaLlegada2);
		this.horaLLegada2 = new SimpleStringProperty(horaLlegada2);
		this.precio = new SimpleStringProperty(precio);
		this.duracion = new SimpleStringProperty(duracion);
		this.delayed = new SimpleStringProperty(delayed);
//		this.href = new SimpleStringProperty(href);
		this.href = new Hyperlink(href);
		
        // action event 
        EventHandler<ActionEvent> event = 
         new EventHandler<ActionEvent>() { 

            public void handle(ActionEvent e) 
            { 
            	mainApp.getHostServices().showDocument(href);
            } 
        }; 
        // when hyperlink is pressed 
        this.href.setOnAction(event); 

		
	}
	public String getDirecto() {
		return directo.get();
	}


	public String getAeropuertoSalida1() {
		return aeropuertoSalida1.get();
	}


	public String getFechaSalida1() {
		return fechaSalida1.get();
	}


	public String getHoraSalida1() {
		return horaSalida1.get();
	}


	public String getAerolinea1() {
		return aerolinea1.get();
	}


	public String getNumVuelo1() {
		return numVuelo1.get();
	}


	public String getAeropuertoLlegada1() {
		return aeropuertoLlegada1.get();
	}


	public String getFechaLlegada1() {
		return fechaLlegada1.get();
	}


	public String getHoraLLegada1() {
		return horaLLegada1.get();
	}


	public String getAeropuertoSalida2() {
		return aeropuertoSalida2.get();
	}


	public String getFechaSalida2() {
		return fechaSalida2.get();
	}


	public String getHoraSalida2() {
		return horaSalida2.get();
	}


	public String getAerolinea2() {
		return aerolinea2.get();
	}


	public String getNumVuelo2() {
		return numVuelo2.get();
	}


	public String getAeropuertoLlegada2() {
		return aeropuertoLlegada2.get();
	}


	public String getFechaLlegada2() {
		return fechaLlegada2.get();
	}


	public String getHoraLLegada2() {
		return horaLLegada2.get();
	}


	public String getPrecio() {
		return precio.get();
	}


	public String getDuracion() {
		return duracion.get();
	}


	public String getDelayed() {
		return delayed.get();
	}


	public Hyperlink getHref() {
		return href;
	}


	@Override
	public String toString() {
		return "LayoutFlight [directo=" + directo + ", aeropuertoSalida1=" + aeropuertoSalida1 + ", fechaSalida1="
				+ fechaSalida1 + ", horaSalida1=" + horaSalida1 + ", aerolinea1=" + aerolinea1 + ", numVuelo1="
				+ numVuelo1 + ", aeropuertoLlegada1=" + aeropuertoLlegada1 + ", fechaLlegada1=" + fechaLlegada1
				+ ", horaLLegada1=" + horaLLegada1 + ", aeropuertoSalida2=" + aeropuertoSalida2 + ", fechaSalida2="
				+ fechaSalida2 + ", horaSalida2=" + horaSalida2 + ", aerolinea2=" + aerolinea2 + ", numVuelo2="
				+ numVuelo2 + ", aeropuertoLlegada2=" + aeropuertoLlegada2 + ", fechaLlegada2=" + fechaLlegada2
				+ ", horaLLegada2=" + horaLLegada2 + ", precio=" + precio + ", duracion=" + duracion + ", delayed="
				+ delayed + ", href=" + href + "]";
	}
}
