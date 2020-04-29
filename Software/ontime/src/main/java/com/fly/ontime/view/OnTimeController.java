package com.fly.ontime.view;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
//import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fly.ontime.db.Mongo;
import com.fly.ontime.load.Loader;
import com.fly.ontime.model.UserData;
import com.fly.ontime.model.Weather;
import com.fly.ontime.predict.Predict;
import com.fly.ontime.python.PyGateway;
import com.fly.ontime.util.AppParams;
import com.fly.ontime.view.model.HyperlinkCell;
import com.fly.ontime.view.model.LayoutFlight;

import javafx.application.HostServices;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Slider;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import static java.nio.file.StandardCopyOption.*;

public class OnTimeController implements Initializable {

	private static Logger _logger = Logger.getLogger(OnTimeController.class);
	
	// Reference to the main application.
    private OnTimeApp mainApp;
    private HostServices hostServices;
    
	
	private static int PRICES_OK = 0;
	private static int PRICES_NOFLIGHTS = 1;
	private static int PRICES_NOCONN = 2;
	private static int PRICES_ERR = 3;
	
    // add your data here from any source 
    private ObservableList<LayoutFlight> flights = FXCollections.observableArrayList();
	
	@FXML
    private ComboBox<String> comboSrc;
	@FXML
    private ComboBox<String> comboDst;
	@FXML
    private TextField txtFieldSrc;
	@FXML
    private TextField txtFieldDst;
	@FXML
    private DatePicker dtFechaVuelo;
	@FXML
    private TextField txtFieldPrecio;
	@FXML
    private TextField txtFieldDuracion;
	@FXML
    private TextField txtFieldRetraso;
	
	@FXML
	private TableView<LayoutFlight> flightsTable;
	
	@FXML
	private TableColumn<LayoutFlight, String> directoColumn;
	
	@FXML
	private TableColumn<LayoutFlight, String> aeropuertoSalida1Column;
	@FXML
	private TableColumn<LayoutFlight, String> fechaSalida1Column;
	@FXML
	private TableColumn<LayoutFlight, String> horaSalida1Column;
	@FXML
	private TableColumn<LayoutFlight, String> aerolinea1Column;
	@FXML
	private TableColumn<LayoutFlight, String> numVuelo1Column;
	@FXML
	private TableColumn<LayoutFlight, String> aeropuertoLlegada1Column;
	@FXML
	private TableColumn<LayoutFlight, String> fechaLlegada1Column;
	@FXML
	private TableColumn<LayoutFlight, String> horaLLegada1Column;
	
	@FXML
	private TableColumn<LayoutFlight, String> aeropuertoSalida2Column;
	@FXML
	private TableColumn<LayoutFlight, String> fechaSalida2Column;
	@FXML
	private TableColumn<LayoutFlight, String> horaSalida2Column;
	@FXML
	private TableColumn<LayoutFlight, String> aerolinea2Column;
	@FXML
	private TableColumn<LayoutFlight, String> numVuelo2Column;
	@FXML
	private TableColumn<LayoutFlight, String> aeropuertoLlegada2Column;
	@FXML
	private TableColumn<LayoutFlight, String> fechaLlegada2Column;
	@FXML
	private TableColumn<LayoutFlight, String> horaLLegada2Column;
	
	
	@FXML
	private TableColumn<LayoutFlight, String> precioColumn;
	@FXML
	private TableColumn<LayoutFlight, String> duracionColumn;
	@FXML
	private TableColumn<LayoutFlight, Hyperlink> hrefColumn;
//	private TableColumn<LayoutFlight, String> hrefColumn;
	@FXML
	private TableColumn<LayoutFlight, String> delayedColumn;
	
    
    /**
     * The constructor.
     * The constructor is called before the initialize() method.
     */
    public OnTimeController() {
    }

    @FXML
    private void handleSearchAction() throws Throwable {

    	_logger.info("Searching flights...");
    	
    	//verificar params
//    	String src = txtFieldSrc.getText();
//    	String dst = txtFieldDst.getText();
    	
    	String src = comboSrc.getValue();
//		src = src.split("\\(")[1].replace(")", "");
    	String dst = comboDst.getValue();
//    	dst = dst.split("\\(")[1].replace(")", "");
    	LocalDate theDate = dtFechaVuelo.getValue();
    	String precio = txtFieldPrecio.getText();
    	String duracion = txtFieldDuracion.getText();
    	String retraso = txtFieldRetraso.getText();
    	
    	//fecha introducida
    	if (theDate == null) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Introduzca fecha");
    		alert.showAndWait();
    		return;
    	}
    	//pesos informados y numéricos
    	if ("".equals(precio) || "".equals(duracion) || "".equals(retraso)) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Introduzca Precio, Duración y Retraso");
    		alert.showAndWait();
    		return;
    	}
    	try {
    		Integer.parseInt(precio);
    		Integer.parseInt(duracion);
    		Integer.parseInt(retraso);
    	} catch (NumberFormatException e) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Precio, Duración y Retraso han de ser valores numéricos");
    		alert.showAndWait();
    		return;
    	}
    	
    	//aeropuertos informados
    	if (src == null || dst == null) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Introduzca los aeropuertos");
    		alert.showAndWait();
    		return;
    	}
    	//aeropuertos diferentes
    	if (src.equals(dst)) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Los aeropuertos de salida y llegada son iguales");
    		alert.showAndWait();
    		return;
    	}  
		src = src.split("\\(")[1].replace(")", "");
    	dst = dst.split("\\(")[1].replace(")", "");
    	String date = AppParams.getFormattter_yyyyMMdd().format(
    			AppParams.getFormattter2_yyyyMMdd().parse(theDate.toString()));
    	
    	//fecha no anterior a 'hoy'
    	Date now = new Date(System.currentTimeMillis());
    	String dateLaunch = AppParams.getFormattter_yyyyMMdd().format(now);
    	if(Integer.valueOf(date) < Integer.valueOf(dateLaunch)) {
    		Alert alert = new Alert(AlertType.ERROR);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Error, la fecha escogida no puede ser inferior a la actual");
    		alert.showAndWait();
    		return;
    	}

//    	//limpiamos tabla
//    	flights.clear();


    	//llamada a python
    	_logger.info("Getting prices...");
    	
    	//csv de precios
		String csvName = src + "_" + dst + "_" + dtFechaVuelo.getValue().toString() + ".csv";
//    	_logger.info("csvName  [" + csvName + "]");

		//borramos fichero de precios si existe. 
		//más abajo se mira si getPrices ha acabado de obtener precios 'frescos' antes de tener que esperar
		//para asegurar que el borrado del fichero se hace antes de que 'java' mire si existe fichero, lo 
		//hacmemos aquí y no en python
		Path csvFile = Paths.get(AppParams.get_pythonDir() + "/csv/xxxxx/" + csvName);
		Path csvFileOld = Paths.get(AppParams.get_pythonDir() + "/csv/xxxxx/" + csvName + ".old");
		if (Files.exists(csvFile)) {
			Files.move(csvFile, csvFileOld, REPLACE_EXISTING);
	    	_logger.info("Eliminado fichero de precios [" + csvName + "]");
		}
		
    	PyGateway.getInstance().getPrices(src, dst, dtFechaVuelo.getValue().toString());
//    	PyGateway.getInstance().getPricesWaiting(src, dst, dtFechaVuelo.getValue().toString());
    	
    	//carga de datos, api, datalake, etc
    	_logger.info("Getting routes data...");

    	UserData userData = new UserData(src.toUpperCase(), dst.toUpperCase(), date, 
				Integer.parseInt(precio), Integer.parseInt(retraso), Integer.parseInt(duracion));
    	_logger.info("UserData entered [" + userData + "]");
    	Loader.getInstance().loadUserRequest(userData);
    	
		//esperar finalización de GetPrices si aun no ha acabado
		
	    if (Files.exists(csvFile)) {
	    	_logger.warn("GetPrices ha acabado antes que rutas, no esperamos.");
	    } else {
	    	_logger.info("Esperando a GetPrices ...");
			int res = waitPricesToEnd(csvName);
			if (res == PRICES_NOFLIGHTS) {
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("OnTime");
				alert.setHeaderText("Buscar vuelo");
				alert.setContentText("No hay vuelos para el origen, destino y fecha introducidos.");
				alert.showAndWait();
				return;
			}  else if (res == PRICES_NOCONN) {
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("OnTime");
				alert.setHeaderText("Buscar vuelo");
				alert.setContentText("No se puede establecer conexión para obtener precios.");
				alert.showAndWait();
				return;
			}  else if (res == PRICES_ERR) {
				Alert alert = new Alert(AlertType.ERROR);
				alert.setTitle("OnTime");
				alert.setHeaderText("Buscar vuelo");
				alert.setContentText("Se produjo un error al buscar vuelos.");
				alert.showAndWait();
				return;
			}  
		}
    	_logger.info("GetPrices finished, continuing with search process...");
    	
    	//llamada a join python
    	_logger.info("Merging routes with prices ...");
    	PyGateway.getInstance().joinDataWaiting(src, dst, dtFechaVuelo.getValue().toString());
    	
    	//predict 
    	_logger.info("Predicting delay ...");
    	
		long t = System.currentTimeMillis();
		//miramos si hay clima en datalake en función de cómo se pueden obtener sus datos de la api de clima
		long tsDeparture = AppParams.strDate2Epoch(
				dtFechaVuelo.getValue().toString() + "T22:30:00.000", AppParams.getFormattter_ISO8601());
		boolean withWeather = Mongo.getInstance().existsWeather(src, tsDeparture);
		
		String suffix = (withWeather) ? "Y" : "N";
		Dataset<Row> file = Predict.fit(AppParams.get_mergeCSVDir() + "/" + csvName, mainApp.spark, withWeather, 
				AppParams.get_modelIndexDir() + suffix, 
				AppParams.get_modelDir() + suffix, 
				AppParams.get_modelName());
    	_logger.info("Finish Ok, total predict.fit time [" + (System.currentTimeMillis() - t) + "]");
//		file.show(19);

    	//relleno de la tabla
    	_logger.info("Generating output ...");
    	if (file.isEmpty()) {
    		Alert alert = new Alert(AlertType.INFORMATION);
    		alert.setTitle("OnTime");
    		alert.setHeaderText("Buscar vuelo");
    		alert.setContentText("Lo sentimos, las rutas involucradas en su viaje no se hayan disponibles en nuestro prototipo");
    		alert.showAndWait();
    		return;
    	}
    	generateOutput(file);
    	
    	_logger.info("Searching flights done.");
    }

    
    private void generateOutput(Dataset<Row> ds) {
    	
//    	_logger.info("---------------------------------------------------------------------------------------");
    	flights.clear();
    	
    	List<Row> aa = ds.collectAsList();
    	aa.forEach(row -> {
    		
    		LayoutFlight lf = new LayoutFlight(mainApp,
//    				row.get(0).toString(),
//    				row.get(1).toString(),
//    				row.get(2).toString(),
    				
    				row.get(7).toString(),
    				
    				row.get(8).toString(),
    				row.get(9).toString(),
    				row.get(10).toString(),
    				row.get(11).toString(),
    				row.get(12).toString(),
    				row.get(13).toString(),
    				row.get(14).toString(),
    				row.get(15).toString(),
    				
					(row.get(35) == null) ? "" : row.get(35).toString(),
					(row.get(36) == null) ? "" : row.get(36).toString(),
					(row.get(37) == null) ? "" : row.get(37).toString(),
					(row.get(38) == null) ? "" : row.get(38).toString(),
					(row.get(39) == null) ? "" : row.get(39).toString(),
					(row.get(40) == null) ? "" : row.get(40).toString(),
					(row.get(41) == null) ? "" : row.get(41).toString(),
					(row.get(42) == null) ? "" : row.get(42).toString(),
							
    				row.get(64).toString(),
    				row.get(65).toString(),
    				row.get(66).toString(),
    				row.get(80).toString()
    				);
//        	_logger.info("[" + lf + "]");
        	flights.add(lf);
    	});
//    	_logger.info("---------------------------------------------------------------------------------------");
    }
    
	public void setData() {
		
		dtFechaVuelo.setValue(LocalDate.now());
		
		//aeropuertos
		List<String> airportsList = Loader.getInstance().getUniversAirportsList();
//		//testing
//        List<String> airportsList = new ArrayList<String>();
//        airportsList.add("BCN");
//        airportsList.add("JFK");//New York
//        airportsList.add("HKG");//Hong Kong
//        airportsList.add("LIM");//Lima
//        airportsList.add("LHR");//London Heathrow
//        airportsList.add("SNY");//Sidney
//        airportsList.add("SFO");//San Francisco
//        airportsList.add("EZE");//Buenos Aires Ezeiza
//        airportsList.add("NRT");//Toquio Narita
//        airportsList.add("CDG");//Paris Charles de Gaulle
//        airportsList.add("ALC");//Alicante
//        airportsList.add("ALG");//
//        airportsList.add("ABJ");//
//        airportsList.add("ABI");//
//        airportsList.add("AUH");//
        
        ObservableList<String> obList = FXCollections.observableList(airportsList);
        comboSrc.getItems().clear();
        comboSrc.setItems(obList);
        //mismos aeropuertos para destino
        List<String> copy = airportsList.stream().collect(Collectors.toList());
        ObservableList<String> obListCopy = FXCollections.observableList(copy);
        comboDst.getItems().clear();
        comboDst.setItems(obListCopy);

        //config tableview
        
        directoColumn.setCellValueFactory(new PropertyValueFactory<>("directo"));
        directoColumn.getStyleClass().add("name-column");
        
        aeropuertoSalida1Column.setCellValueFactory(new PropertyValueFactory<>("aeropuertoSalida1"));
        fechaSalida1Column.setCellValueFactory(new PropertyValueFactory<>("fechaSalida1"));
        horaSalida1Column.setCellValueFactory(new PropertyValueFactory<>("horaSalida1"));
        aerolinea1Column.setCellValueFactory(new PropertyValueFactory<>("aerolinea1"));
        numVuelo1Column.setCellValueFactory(new PropertyValueFactory<>("numVuelo1"));
        aeropuertoLlegada1Column.setCellValueFactory(new PropertyValueFactory<>("aeropuertoLlegada1"));
        fechaLlegada1Column.setCellValueFactory(new PropertyValueFactory<>("fechaLlegada1"));
        horaLLegada1Column.setCellValueFactory(new PropertyValueFactory<>("horaLLegada1"));
		
        aeropuertoSalida2Column.setCellValueFactory(new PropertyValueFactory<>("aeropuertoSalida2"));
        fechaSalida2Column.setCellValueFactory(new PropertyValueFactory<>("fechaSalida2"));
        horaSalida2Column.setCellValueFactory(new PropertyValueFactory<>("horaSalida2"));
        aerolinea2Column.setCellValueFactory(new PropertyValueFactory<>("aerolinea2"));
        numVuelo2Column.setCellValueFactory(new PropertyValueFactory<>("numVuelo2"));
        aeropuertoLlegada2Column.setCellValueFactory(new PropertyValueFactory<>("aeropuertoLlegada2"));
        fechaLlegada2Column.setCellValueFactory(new PropertyValueFactory<>("fechaLlegada2"));
        horaLLegada2Column.setCellValueFactory(new PropertyValueFactory<>("horaLLegada2"));
		
        precioColumn.setCellValueFactory(new PropertyValueFactory<>("precio"));
        duracionColumn.setCellValueFactory(new PropertyValueFactory<>("duracion"));
        delayedColumn.setCellValueFactory(new PropertyValueFactory<>("delayed"));
        
        hrefColumn.setCellValueFactory(new PropertyValueFactory<>("href"));
        hrefColumn.setCellFactory(new HyperlinkCell());
//        nameColumn.setCellValueFactory(new PropertyValueFactory<>("websiteName"));
//        hrefColumn.setCellValueFactory(new PropertyValueFactory<>("hyperlink"));
//        hrefColumn.setCellFactory(new HyperlinkCell());
        
        //add your data to the table here.
        flightsTable.setItems(flights);
	}
    
	@Override
	public void initialize(URL location, ResourceBundle resources) {
    	setData();
	}
	/**
	 * Is called by the main application to give a reference back to itself.
	 * 
	 * @param mainApp
	 */
	public void setMainApp(OnTimeApp mainApp) {
		this.mainApp = mainApp;
	}
	
    public HostServices getHostServices() {
        return hostServices ;
    }
    public void setHostServices(HostServices hostServices) {
        this.hostServices = hostServices ;
    }
	
    
    private int processResult(String csvFileName, String fileName) {
    	int ret = PRICES_OK;
		if (csvFileName.equals(fileName)) {
		} else if (fileName.endsWith(".noflights")) {
			ret = PRICES_NOFLIGHTS;
		} else if (fileName.endsWith(".noconnex")) {
			ret = PRICES_NOCONN;
		} else if (fileName.endsWith(".error")) {
			ret = PRICES_ERR;
		}
		return ret;
    }

    	
	/**
	 * Espera proceso python de obtencion de precios
	 * 
	 * @param csvFileName
	 * @throws IOException
	 * @throws InterruptedException
	 */
    private int waitPricesToEnd(String csvFileName) throws IOException, InterruptedException {
    	
    	int result = PRICES_OK;
    			
		Path pricesFolder = Paths.get(AppParams.get_pythonDir() + "/csv/xxxxx");
		WatchService watchService = FileSystems.getDefault().newWatchService();
		pricesFolder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

		boolean valid = true;
		do {
			WatchKey watchKey = watchService.take();

			for (WatchEvent event : watchKey.pollEvents()) {
				WatchEvent.Kind kind = event.kind();
				
				if (StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind())) {
					String fileName = event.context().toString();
					valid = false;
					_logger.info("Prices file CREATED [" + fileName + "]");
					result = processResult(csvFileName, fileName);
					break;
				}
				if (StandardWatchEventKinds.ENTRY_MODIFY.equals(event.kind())) {
					String fileName = event.context().toString();
					valid = false;
					_logger.info("Prices file MODIFIED [" + fileName + "]");
					result = processResult(csvFileName, fileName);
					break;
				}
			}
//			valid = watchKey.reset();
		} while (valid);
		return result;
    }
	
	public static void main(String[] args) throws Throwable {
		
		long tsDeparture = AppParams.strDate2Epoch("2019-10-02T22:30:00.000", AppParams.getFormattter_ISO8601());
		boolean withWeather = Mongo.getInstance().existsWeather("AAE", tsDeparture);
		_logger.info("withWeather [" + withWeather + "]");
	}
		
}
