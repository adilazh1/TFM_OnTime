package com.fly.ontime.view;


import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import com.fly.ontime.db.Mongo;
import com.fly.ontime.view.model.LayoutFlight;
import com.fly.ontime.view.model.LayoutUserData;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/**
 * 
 * @author OnTime
 *
 */
public class OnTimeApp extends Application {

	private static Logger _logger = Logger.getLogger(OnTimeApp.class);

	private static String HADOOP_COMMON_PATH = "C:\\Users\\adilazh1\\eclipse-workspace\\ontime\\src\\main\\resources\\winutils";
	
	protected static SparkSession spark;

	
    private Stage primaryStage;
    private Pane rootLayout;
    
    private LayoutUserData userData;
    
    
    public OnTimeApp() {
		super();
	}

    
	@Override
    public void stop() {
    	_logger.info("Ontime stopped");
    	Mongo.getInstance().closeMongoClient();
    	
		spark.close();
    }    
    
    @Override
    public void start(Stage primaryStage) {
    	
        this.primaryStage = primaryStage;
        this.primaryStage.setTitle("OnTime");
        initRootLayout();
    }
    
    /**
     * Initializes the root layout.
     */
    public void initRootLayout() {
        try {
        	userData = new LayoutUserData();
        	
            // Load root layout from fxml file.
            FXMLLoader loader = new FXMLLoader();
//            loader.setLocation(OnTimeApp.class.getResource("OnTime2.fxml"));
            ClassLoader classLoader = OnTimeApp.class.getClassLoader();
            loader.setLocation(classLoader.getResource("OnTime2.fxml"));
            rootLayout = (Pane) loader.load();
            
            OnTimeController myController = loader.getController();
            myController.setMainApp(this);
            myController.setHostServices(getHostServices());
            
            // Show the scene containing the root layout.
            Scene scene = new Scene(rootLayout);
            primaryStage.setScene(scene);
            primaryStage.show();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    /**
     * Returns the main stage.
     * @return
     */
    public Stage getPrimaryStage() {
        return primaryStage;
    }

    public static void main(String[] args) {
    	
    	System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
    	
		// Spark sesion
		spark = SparkSession.builder().master("local[*]").appName("Model")
				.config("spark.rdd.compress", true)
				.config("spark.executor.memory", "3G")
				.config("spark.driver.memory", "3G")
				.getOrCreate();

    	_logger.info("Lanzando aplicación...");
        launch(args);
    }
}