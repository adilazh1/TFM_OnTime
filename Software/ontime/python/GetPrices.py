
import json
import itertools
import pandas as pd
import Scraper
import re
import sys
import time as t
import logging.config

path_airlines = "C:/Users/adilazh1/eclipse-workspace/ontime/python/activeAirlines.json"
path_prices = "C:/Users/adilazh1/eclipse-workspace/ontime/python/csv/xxxxx"
path_log = "C:/Users/adilazh1/eclipse-workspace/ontime/python/"


# Parametros: Fecha, aeropuerto de llegada y aeropuertos de salida
# Funcion: Procesamiento de la informacion recogida en xxxxx y condensarla en un CSV

def getPrices(arrival,departure,date):

    data=Scraper.Scrap(logger, arrival,departure,date)

#   en caso de que no haya sido posible acceder a los vuelos se devuelve un 1, escribir CSV vacio y terminar
    if isinstance(data,int):


        if (data == 1):
            dic = {"A.Salida": [], "A.Llegada": [], "Duracion": [], "Paradas": [],
               "Vuelos": [], "Vuelos alternativos": [], "precio": [],
               "Escala": [], "hSalida_v1": [], "hLlegada_v1": [],
               "hSalida_v2": [], "hLlegada_v2": [], "Enlaces": []}

        pd.DataFrame(dic).to_csv(path_prices + "/" + arrival + "_" + departure + "_" + date + ".csv.noconnex", sep=";")
        return





    begining=t.time()
    #######################################

    # Eliminar vuelo/tren de la cabecera si lo hay..

    if len(data)>0:

        data = data[(data["hSalida_v1"] != "null") | (data["hSalida_v2"] != "null") | (data["hLlegada_v1"] != "null") | (
                data["hSalida_v1"] != "null")]
        data = data.reset_index(drop=True)


    logger.info("Making flghts available...")

    ############################################
    # Cargar diccionario

    dic = {}
    j = json.load(open(path_airlines))
    airlines = j.get("airlines")
    for i in range(0, len(airlines)):
        dic[str(airlines[i].get("name"))] = str(airlines[i].get("iata"))

    #############################################

    # Canviar nombre de la aerolinea por codigo IATA

    for i in range(0, data.shape[0]):
        for j in range(0, len(data["Vuelos"][i])):
            if len(data["Vuelos"][i]) != 0:
                try:
                    words = re.findall("[A-Za-zÀ-ÿ]{1,}", data["Vuelos"][i][j])
                    numbers = re.findall("[0-9]{1,}", data["Vuelos"][i][j])[0]
                    name = ""
                    for word in words:
                        name = name + " " + word

                    expression = dic.get(name.strip()) + " " + numbers
                    data["Vuelos"][i][j] = str(expression)

                except TypeError:

                    expression = "XXX" + " " + numbers
                    data["Vuelos"][i][j] = str(expression)

        for j in range(0, len(data["Vuelos alternativos"][i])):
            if len(data["Vuelos alternativos"][i]) != 0:
                data["Vuelos alternativos"][i][j] = data["Vuelos alternativos"][i][j].strip()


    #logger.info("flights with more than a scale dropped, dictionary used successfully")
#     print("flights with more than a scale dropped, dictionary used successfully")
    ##################################################

    # Separar los vuelos directos con un codeshare, para ellos no aplica el algoritmo de combinatoria

    direct_guys = [i for i in range(0, len(data)) if
                   (len(data["Vuelos"][i]) == 1) & (len(data["Vuelos alternativos"][i]) == 1)]

    data_directs = data.loc[direct_guys][:]
    data = data.drop(axis=0, index=direct_guys)
    data = data.reset_index(drop=True)


    data["Vuelos"] = data["Vuelos"] + data["Vuelos alternativos"]


    ###################################################
    # Algoritmo de combinatoria, se crean todos los pares de vuelos posibles en los vuelos con codeshare


    cols = ["Vuelos","A.Llegada","A.Salida","Duracion","Enlaces","Escala","hSalida_v1",
      "hLlegada_v1","hSalida_v2","hLlegada_v2","Paradas","Vuelos alternativos","precio"]
    for k in range(0, data.shape[0]):
        dic = {}
        for col in cols:
            dic[col] = []

        if len(data["Vuelos alternativos"][k]) > 0 :
            items = itertools.permutations(data["Vuelos"][k], 2)
            breaker = True
            while breaker:
                try:
                    for col in cols:
                        if col == "Vuelos":
                            dic[col].append(list(items.__next__()))
                        else:
                            dic[col].append(data[col][k])

                except StopIteration:
                    breaker = False

        else:
            for col in cols:
                dic[col].append(data[col][k])

        if k == 0:
            df = pd.DataFrame(dic)

        else:
            temp = pd.DataFrame(dic)
            frame = [df, temp]
            df = pd.concat(frame)



#     print("combinatorics of codeshares worked fine")
    #########################################################

    # si el dataset de xxxxx no esta vacio, el dataset resultado (df) se habra construido, en caso de que no sea asi, escribir dataset vacio y terminar

    if len(data)>0:
        dfclean = df.reset_index().drop(axis=1, columns=["index"])

        dfclean = pd.concat([data_directs, dfclean], sort=False)
        dfclean = dfclean.reset_index(drop=True)




    ########################################################

        # Separacion por columnas de los pares de vuelos

        v2 = []

        for i in range(0, dfclean.shape[0]):
            if len(dfclean["Vuelos"][i]) > 1:
                v2.append(dfclean["Vuelos"][i][1])
                dfclean["Vuelos"][i].remove(dfclean["Vuelos"][i][1])
                dfclean["Vuelos"][i] = dfclean["Vuelos"][i][0]
            else:
                v2.append("null")
                dfclean["Vuelos"][i] = dfclean["Vuelos"][i][0]

        dfclean["Vuelos1"] = v2



    ##########################################################

        dic = {}
        cols = ["Code_v1", "flightnumber_v1", "Code_v2", "flightnumber_v2"]
        for col in cols:
            dic[col] = []



    ############################################################

        for i in range(0, dfclean.shape[0]):
            e0 = dfclean["Vuelos"][i].split(" ")
            e1 = dfclean["Vuelos1"][i].split(" ")
            dic["Code_v1"].append(e0[0])
            dic["flightnumber_v1"].append(e0[1])
            if dfclean["Vuelos1"][i] != "null":
                dic["Code_v2"].append(e1[0])
                dic["flightnumber_v2"].append(e1[1])

            else:
                dic["Code_v2"].append("null")
                dic["flightnumber_v2"].append("null")

        partition = pd.DataFrame(dic)



    #########################################################

        # Escribir el CSV

        dfclean = pd.merge(right=partition, left=dfclean, right_index=True, left_index=True)
        dfclean = dfclean.drop(axis=1, columns=["Vuelos", "Vuelos1"])

        dfclean.to_csv(path_prices+"/"+arrival+"_"+departure+"_"+date+".csv",sep=";")
        timing=t.time()-begining
        logger.info("CSV written successfully, transformations took {} seconds".format(timing))

    else:

        data.to_csv(path_prices+"/"+arrival+"_"+departure+"_"+date+".csv.noflights",sep=";")
        timing = t.time() - begining
        logger.info("CSV written successfully but  with no flights in it and it took {} seconds".format(timing))



if __name__ == "__main__":
    # Get the total number of args passed to the demo.py
    total = len(sys.argv)
    # Get the arguments list
    cmdargs = str(sys.argv)
    print ("The total numbers of args passed to the script: %d " % total)
    print ("Args list: %s " % cmdargs)
    print ("src: %s " % sys.argv[1])
    print ("dst: %s " % sys.argv[2])
    print ("date: %s " % sys.argv[3])

    logging.config.fileConfig(path_log + 'logging.ini')
    logger = logging.getLogger('OnTime')

    logger.info("------------------------------------------------------------------------------------------------------------------------")
    logger.info("GETPRICES from [" + cmdargs + "]")
    try:
        getPrices(sys.argv[1],sys.argv[2],sys.argv[3])
    except:
        logger.warning("An unexpected error ocurred")
        dic = {"A.Salida": [], "A.Llegada": [], "Duracion": [], "Paradas": [],
               "Vuelos": [], "Vuelos alternativos": [], "precio": [],
               "Escala": [], "hSalida_v1": [], "hLlegada_v1": [],
               "hSalida_v2": [], "hLlegada_v2": [], "Enlaces": []}

        pd.DataFrame(dic).to_csv(path_prices + "/" + sys.argv[1] + "_" + sys.argv[2]+ "_" + sys.argv[3] + ".csv.error", sep=";")

    logger.info("GETPRICES process finished.")
    logger.info("------------------------------------------------------------------------------------------------------------------------")







