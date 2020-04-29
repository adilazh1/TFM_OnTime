# OnTime
OnTime consiste en una idea de negocio presentada como TFM en Big Data.  
  
  Desarrolla una parte dedicada a la viabilidad del negocio y estrategias de **Marketing**, Otra parte dedicada a **Data Management** desde ingesta, storage y arquitectura de BBDD para generar datos explotables por Analytics Layer.  
  En una tercera parte, explota los datos almacenados para entrenar modelos de **Machine Learning** y usarlos para predicciones en real time.  
    
OnTime es un prototipo de buscador de vuelos donde el objetivo, a parte de ofrecer lista de vuelos disponibles al usuario, es analizar las posibles causas que pueden provocar delays en la salida de los vuelos (vuelos próximos y no muy lejanos en el tiempo) con la idea de ofrecer al usuario una predicción sobre el retraso y permitirle usar un filtrado ponderado, donde le asigna pesos a retraso y también a precio y duración.  
  
  Para más información tiene a su disposición la [memoria.](https://github.com/adilazh1/TFM_OnTime/blob/master/Memoria/OnTime.pdf)  
  
  OnTime usa la filosofía de **DataLake**  para guardar los datos en un lago con estructura de sistema de ficheros y **MongoDB** como base de datos que alberga los datos ya formateados según nuestro interés.  
  
  En cuanto a procesamiento de datos usa **Spark SQL** y **Spark MLlib** y como interfaz gráfica hace uso de **JavaFX**.  
  
  El motor de entrenamiento de los datos, proceso que más tiempo de cómputo conlleva, ha sido probado en un **Cluster** de 2 slaves y un master ofrecido por el CPD de la universidad.  
  
## Estructura general 
![](https://github.com/adilazh1/TFM_OnTime/blob/master/esquema_ontime.png?raw=true)
### Componentes
##### airportWeather
Proceso batch que se lanza cada 5 días para actualizar datos del clima de los aeropuertos.
##### routeRatings 
Proceso batch que se lanza cada 15 días para actualizar estadísticas sobre el desempeño de las aerolíneas y aeropuertos. Estadísticas que se usan en el entrenamiento de modelos.
##### dataset
Proceso batch que se encarga de volcar ciertos datos almacenados en MongoDB en un fichero csv que usamos en entrada para el programa que se encarga de entrenar modelos. En este proceso se hace hincapié en la optimización. es un proceso que requiere tiempo por lo que las desnormalizaciones en los documentos json están pensadas para lecturas rápidas por parte de este proceso. También se crean índices para mejorar la lectura. En una primera versión tomaba algo mas que 40 minutos de media, con un mejor diseño de los documentos json y elección de índices apropiados, ha bajado a tomar poco más que 3 minutos.  
Es un proceso que se lanza a demanda cuando se quiera re-entrenar algún modelo con más datos nuevos.
##### Model
Proceso Batch que toma el fichero de salida del programa dataset (que podemos insertar en HDFS en el Cluster) y entrenar diferentes algoritmos de Machine Learning especificando el algoritmo en el fichero properties asociado al programa, dejándonos el modelo entrenado a usar en real time.  
  
Modelos implementados:  
-RandomForests  
-RandomForests with CV    
-Gradient-Boosted tree classifier  
-Support Vector Machine  
-OneVsRest Support Vector Machine   
-OneVsRest regression  
-Naiv Bayes  
  
También sobre-escribe un csv de estadísticas de los diferentes algoritmos para así tener accuracy, tiempos, etc de cada algoritmo usado.
##### routeStatus
Proceso Batch diario que nos permite almacenar toda la información de los vuelos del día anterior usando un fichero de rutas aéreas en entrada. También podemos bajar datos históricos de un día en concreto o intervalo especificándolo en el fichero de parámetros asociado al programa.
##### routeStatusSpark
Versión de routeStatus que aprovecha paralelismo haciendo consultas a la API en paralelo tratando el fichero de rutas de manera distribuida.
##### ontime
Es el programa kernel del proyecto, ofrece interfaz gráfica, permite buscar vuelos futuros y devuelve al usuario lista de vuelos para su ruta y fecha dadas permitiéndole el acceso a la web del proveedor del vuelo para la reserva.  
También hace llamadas síncronas y asíncronas a módulos Python encargados de enriquecer el dataset con nueva información a la que se accede en real time y que no está almacenada previamente, cómo los datos de precios.
## Requisitos
Dado que el proyecto no se mantiene ni está alojado en ningún servidor, si desea hacer pruebas con el mismo va a necesitar de implementar la infraestructura usted mismo.  
-Obtener claves de acceso de las **APIs** con datos de vuelos (FlightStats Cirium) y con datos del clima (openWeatherMap) .  
-Tener MongoDB instalado.  
-Configurar [confOnTime.xml](https://github.com/adilazh1/TFM_OnTime/blob/master/Software/ontime/src/main/resources/confOnTime.xml) con las claves de las APIs, y las rutas hacia los modelos entrenados, los módulos Python, etc.  
-Configurar los ficheros properties de los procesos que sirven para bajar datos e insertarlos en mongoDB.  
-Tener Python 3.  
-Java 8.  
-Maven
## Demo
![](https://github.com/adilazh1/TFM_OnTime/blob/master/ontime_launcher.png?raw=true)

El proyecto ha sido desarrollado por:  
- Raúl Bautista Erustes
- Adria Canton Aynes
- Adil Ziani El Ali