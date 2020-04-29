
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import ElementClickInterceptedException
import time as t
import re



# path_gecko = "/usr/local/Cellar/geckodriver/0.24.0/bin"

path_gecko = "C:/Users/AMIGO/eclipse-workspace/ontime/python/"

#Parametros:  Fecha, aeropuerto de llegada y aeropuertos de salida
# Funcion: Connexion con xxxxx y escrapeo de la informacion de vuelos

def Scrap(logger,departure,arrival,date):

    begining = t.time()
    url="https://www.xxxxx.es/flights/"+departure+"-"+arrival+"/"+date+"?sort=depart_a&fs=stops=-2"

   # Conectar Selenium con xxxxx, interactuar con la ventana de cookies, en caso de interactuar correctamente, salir del bucle
   # , en caso contrario repetir el proceso hasta alcanzar un número de intentos, devolver 1 si este número excede.


    breaker= True
    while breaker:
        rep=0

        try:


            firefox_options = Options()
            firefox_options.add_argument('--headless')
            firefox_options.add_argument('--disable-gpu')
            driver = webdriver.Firefox(path_gecko,options=firefox_options)
            driver.get(url)
            t.sleep(5)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            try:


                dialog = soup.find_all("div", class_="_ixe")[0]
                blank_spaces = dialog.find_all(text=re.compile("\n"))
                for blank in blank_spaces:
                    new = blank.replace("\n", "")
                    blank.replace_with(new)
                dic = dialog.contents[1].attrs
                id_button = str(dic['id'])
            except (IndexError, TimeoutException):
                logger.info("waiting for webpage to show flights..")
#               print("first window not showing")



            try:


                dialog = soup.find("div", class_="_jg- _ioG")
                blank_spaces = dialog.find_all(text=re.compile("\n"))
                for blank in blank_spaces:
                    new = blank.replace("\n", "")
                    blank.replace_with(new)
                dic = dialog.contents[1].attrs
                id_button = str(dic['id'])
            except (AttributeError, TimeoutException):
              #print("second window not showing")
                logger.info("waiting for webpage to show flights..")






            button_cookie = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.ID, id_button)))
            button_cookie.click()
            t.sleep(5)
            breaker=False



        except (TimeoutException,UnboundLocalError):
            logger.info("connection failed, connecting again...")
            rep=rep+1
            driver.close()

            if rep>5:
                break



            t.sleep(10)


    ####################################################

    if rep>5:
        logger.warn("Sorry, an error ocurred on getting flights")
        return 1




    #  Scrollear la pagina encontrando el boton para desplegar hasta que no se encuentre, en caso de que otro elemento lo oculte, seguir el bucle

    breaker = True
    n = 1
    while breaker:
        try:
            number = 3500 * n
            driver.execute_script("window.scrollTo(0," + str(number) + ")")
            button_load = WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.CLASS_NAME, "moreButton")))
            button_load.click()
            t.sleep(3)
            n = n + 1
        except TimeoutException:
#           print("page already scrolled")
            logger.info("page already scrolled")

            breaker = False

        except ElementClickInterceptedException:

            logger.info("element intercepted, let's try again")
#           print("element intercepted, let's try again")






    #######################################################################



    soup = BeautifulSoup(driver.page_source, "html.parser")
    times = soup.find_all("div", class_="section times")
    duration = soup.find_all("div", class_="section duration")
    stops = soup.find_all("div", class_="section stops")
    driver.close()


    #######################################################################

    # Recoger IATA de llegada y salida junto a la duracion del vuelo

    llista_dades = []

    for i in range(0, len(duration)):
        blank_spaces = duration[i].find_all(text=re.compile("\n"))
        for blank in blank_spaces:
            new = blank.replace("\n", "")
            blank.replace_with(new)

        duracion = duration[i].contents[1].string
        Asalida = duration[i].contents[3].contents[1].string
        Allegada = duration[i].contents[3].contents[5].string

        llista_dades.append([str(duracion), str(Asalida), str(Allegada)])



    #######################################################################

    # Recoger precios

    prices = soup.find_all("div", class_="multibook-dropdown")
    prices_list = []
    for i in range(0, len(prices)):
        p = prices[i].find("span", class_="price-text")
        blank_spaces = p.find_all(text=re.compile("\n"))
        for blank in blank_spaces:
            new = blank.replace("\n", "")
            blank.replace_with(new)
        prices_list.append((p.contents[0].split("\xa0€")[0]))


    #########################################################################

    # En caso de Vuelos/Trenes sin desplegable en la cabecera de la pagina, recoger el precio

    header_prices = []
    headers = soup.find_all("div", class_="aboveResultsContainer")

    for header in headers:
        try:
            price_headers = header.find("span", class_="price option-text")
            blank_spaces = price_headers.find_all(text=re.compile("\n"))
            for blank in blank_spaces:
                new = blank.replace("\n", "")
                blank.replace_with(new)

            header_prices.append(price_headers.contents[0].split("\xa0€")[0])

        except AttributeError:
            logger.info("No issues in header detected")




    prices_list = header_prices + prices_list



    #########################################################################

    # Recoger ciudades intermedias

    cities = []

    for stop in stops:
        blank_spaces = stop.find_all(text=re.compile("\n"))
        for blank in blank_spaces:
            new = blank.replace("\n", "")
            blank.replace_with(new)
        if len(stop.find_all("span", class_="js-layover")) == 1:
            if len(stop.find_all("span", class_="warn")) == 0:
                city = str(stop.contents[3].contents[1].contents[0])
            else:
                city = str(stop.contents[3].contents[1].contents[0].contents[0])

        elif len(stop.find_all("span", class_="js-layover")) == 2:
            city = str(stop.contents[3].contents[1].contents[0]) + "," + str(stop.contents[3].contents[3].contents[0])
        else:
            city = "NA"
        cities.append(city)


    ###########################################################################

    # Recoger Vuelos

    list_of_flights = []
    alternative_names = []
    general = soup.find_all("div", class_="resultWrapper")
    for j in range(0, len(general)):

        planes = general[j].find_all("div", "planeDetails details-subheading")

        companies = []
        for i in range(0, len(planes)):
            blank_spaces = planes[i].find_all(text=re.compile("\n"))
            # bucle dedicat als espais en blanc
            for blank in blank_spaces:
                new = blank.replace("\n", "")
                blank.replace_with(new)
            companies.append(str(planes[i].contents[0].split("·")[0]))
        list_of_flights.append(companies)


    #############################################################################



    list_of_stops = []
    for stop in stops:
        blank_spaces = stop.find_all(text=re.compile("\n"))
        for blank in blank_spaces:
            new = blank.replace("\n", "")
            blank.replace_with(new)
        list_of_stops.append(str(stop.contents[1].string))


    ###################################################################


    # Recoger Codeshares

    general = soup.find_all("div", class_="resultWrapper")
    alternative = []
    # general.find("div",class_="_iqC _ijL _iak")
    for i in range(0, len(general)):
        other = []
        try:
            other_numbers = general[i].find("div", class_="_iqC _ijL _iak")
            alternative_flights = str(other_numbers.contents[0]).replace("Comercializado como", "")
            for flight in alternative_flights.split(","):
                other.append(flight)
            alternative.append(other)

        except AttributeError:
            alternative.append(other)



     ##################################################################

    # Recoger horarios, ignorar los horarios de los vuelos con mas de 1 escala

    departure_v1 = []
    arrival_v1 = []
    departure_v2 = []
    arrival_v2 = []

    details = soup.find_all("div", class_="resultWrapper")

    for i in range(0, len(details)):
        times = details[i].find_all("div", class_="segmentTimes details-heading text-row")
        for time in times:
            blank_spaces = time.find_all(text=re.compile("\n"))
            for blank in blank_spaces:
                new = blank.replace("\n", "")
                blank.replace_with(new)

        if len(times) == 1:

            departure_v1.append(times[0].contents[1].contents[0])
            arrival_v1.append(times[0].contents[3].contents[0])
            departure_v2.append("null")
            arrival_v2.append("null")
        elif len(times) == 2:
            departure_v1.append(times[0].contents[1].contents[0])
            arrival_v1.append(times[0].contents[3].contents[0])
            departure_v2.append(times[1].contents[1].contents[0])
            arrival_v2.append(times[1].contents[3].contents[0])
        else:
            departure_v1.append("null")
            arrival_v1.append("null")
            departure_v2.append("null")
            arrival_v2.append("null")

    ##################################################################

    # Recoger hrefs

    general = soup.find_all("div", class_="resultWrapper")
    hrefs = []
    for i in range(0, len(general)):
        element = general[i].find("div", class_="multibook-dropdown").find_all("a", class_="booking-link", role="button")[0]
        blank_spaces = element.find_all(text=re.compile("\n"))
        for blank in blank_spaces:
            new = blank.replace("\n", "")
            blank.replace_with(new)
        href = str(element.attrs["href"])
        match = re.match("https://www.xxxxx.es/", href)
        if match is None:
            href = "https://www.xxxxx.es" + href

        hrefs.append(href)

     #############################################

    # Comprobar que, debido a los precios en cabecera, las listas del dataset coincidan



    if len(llista_dades) != len(list_of_flights):
        difference = len(llista_dades) - len(list_of_flights)
        for i in range(0, difference):
            list_of_flights.insert(0, ["null"])
            alternative.insert(0, ["null"])
            hrefs.insert(0, ["null"])
            departure_v1.insert(0, "null")
            arrival_v1.insert(0, "null")
            departure_v2.insert(0, "null")
            arrival_v2.insert(0, "null")


    #################################################################

    asalida = [llista_dades[i][1] for i in range(0, len(llista_dades))]
    allegada = [llista_dades[i][2] for i in range(0, len(llista_dades))]
    Duracion = [llista_dades[i][0] for i in range(0, len(llista_dades))]


    # Construccion del dataset a partir de las listas

    dic = {"A.Salida": asalida, "A.Llegada": allegada, "Duracion": Duracion, "Paradas": list_of_stops,
           "Vuelos": list_of_flights, "Vuelos alternativos": alternative, "precio": prices_list,
           "Escala": cities, "hSalida_v1": departure_v1, "hLlegada_v1": arrival_v1,
           "hSalida_v2": departure_v2, "hLlegada_v2": arrival_v2, "Enlaces": hrefs}






    data = pd.DataFrame(dic)
    timing=t.time()-begining
#     print("Data scraped successfully, Number of rows is {} and was {} seconds long".format(data.shape[0],timing))
    logger.info("Data scraped successfully, Number of rows is {} and was {} seconds long".format(data.shape[0],timing))

    return data









