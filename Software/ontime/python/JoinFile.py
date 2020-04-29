import pandas as pd
import sys
import numpy as np
import logging.config
from pymongo import MongoClient


path_prices="C:/Users/adilazh1/eclipse-workspace/ontime/python/csv/kayak"
path_csv_routes="C:/Users/adilazh1/eclipse-workspace/ontime/python/csv/routes"
path_final_routes="C:/Users/adilazh1/eclipse-workspace/ontime/python/csv/merged"
path_log = "C:/Users/adilazh1/eclipse-workspace/ontime/python/"

def doJoin(departure,arrival,date):

    # Carga del dataset relativo a los precios

    dtype = {"flightnumber_v2": pd.Int64Dtype()}
    df_kayak = pd.read_csv(path_prices+"/"+departure+"_"+arrival+"_"+date+".csv",
                           sep=";", dtype=dtype)


    useful_cols=["hSalida_v1","hLlegada_v1","hSalida_v2","hLlegada_v2","precio","Code_v1","flightnumber_v1","Code_v2","flightnumber_v2","Enlaces","Duracion"]
    df_kayak=df_kayak.loc[:,useful_cols]
    renames={"Code_v1":"carrierFsCode_v1","Code_v2":"carrierFsCode_v2","flightnumber_v1":"flightNumber_v1","flightnumber_v2":"flightNumber_v2","Enlaces":"enlaces","Duracion":"duracion"}
    df_kayak=df_kayak.rename(columns=renames)
    logger.info("From kayak CSV : {}".format(df_kayak.shape[0]))

######################################################


    for i in range(0, len(df_kayak)):
        if len(df_kayak["hLlegada_v1"][i]) == 4:
            df_kayak.loc[i, "hLlegada_v1"] = "0" + df_kayak.loc[i, "hLlegada_v1"]

        if len(df_kayak["hSalida_v1"][i]) == 4:
            df_kayak.loc[i, "hSalida_v1"] = "0" + df_kayak.loc[i, "hSalida_v1"]

        if (str(df_kayak["hLlegada_v2"][i]) != "nan") & (len(str(df_kayak["hLlegada_v2"][i])) == 4):
            df_kayak.loc[i, "hLlegada_v2"] = "0" + df_kayak.loc[i, "hLlegada_v2"]

        if (str(df_kayak["hSalida_v2"][i]) != "nan") & (len(str(df_kayak["hSalida_v2"][i])) == 4):
            df_kayak.loc[i, "hSalida_v2"] = "0" + df_kayak.loc[i, "hSalida_v2"]



##########################################################
   # Carga del dataset recibido de Flightstats

    dtype = {"flightNumber_v1": pd.Int64Dtype(), "flightNumber_v2": pd.Int64Dtype()}

    df=pd.read_csv(path_csv_routes+"/"+departure+"_"+arrival+"_"+date+".csv",sep=";",index_col=False,dtype=dtype,usecols=range(38))

    logger.info("From CSV FLightstats: {} ".format(len(df)))
#########################################################

    directs=df[df["directo"]==True]
    scale=df[df["directo"]==False]

    scale_nulls=scale[(scale["flightNumber_v1"].isnull()) | (scale["flightNumber_v2"].isnull())]
    scale_numbers=scale[(~(scale["flightNumber_v1"].isnull())) & (~(scale["flightNumber_v2"].isnull()))]
    directs_nulls=directs[directs["flightNumber_v1"].isnull()]
    directs_numbers=directs[(~(directs["flightNumber_v1"].isnull()))]

#########################################################

#1er join ( vuelos con escala y sin nulls)

    joiner_1=["carrierFsCode_v1","carrierFsCode_v2","flightNumber_v1","flightNumber_v2","hSalida_v1","hLlegada_v1","hSalida_v2","hLlegada_v2"]

    df_kayak["auxiliar_index"]=range(0,len(df_kayak))
    join_scale_numbers=pd.merge(left=df_kayak,right=scale_numbers,left_on=joiner_1,right_on=joiner_1)

    to_drop=pd.Series(join_scale_numbers["auxiliar_index"].values).unique()
    df_kayak=df_kayak.drop(axis=0,index=to_drop.tolist())


# 2n join ( vuelos con escala y con nulls)

    joiner_2=["carrierFsCode_v1","carrierFsCode_v2","hSalida_v1","hLlegada_v1","hSalida_v2","hLlegada_v2"]
    join_scale_nulls=pd.merge(left=df_kayak,right=scale_nulls,left_on=joiner_2,right_on=joiner_2,
                          suffixes=("_py",""))

    to_drop=pd.Series(join_scale_nulls["auxiliar_index"].values).unique()
    df_kayak=df_kayak.drop(axis=0,index=to_drop.tolist())

# 3er join ( vuelos directos sin nulls)

    joiner_3=["carrierFsCode_v1","carrierFsCode_v2","flightNumber_v1","flightNumber_v2","hSalida_v1","hLlegada_v1","hSalida_v2","hLlegada_v2"]
    join_directs_numbers=pd.merge(left=df_kayak,right=directs_numbers,left_on=joiner_3,right_on=joiner_3)

    to_drop=pd.Series(join_directs_numbers["auxiliar_index"].values).unique()
    df_kayak=df_kayak.drop(axis=0,index=to_drop.tolist())

# 4to join ( vuelos directos con nulls)

    joiner_4=["carrierFsCode_v1","carrierFsCode_v2","flightNumber_v2","hSalida_v1","hLlegada_v1","hSalida_v2","hLlegada_v2"]
    join_directs_nulls=pd.merge(left=df_kayak,right=directs_nulls,left_on=joiner_4,right_on=joiner_4,
                           suffixes=("_py",""))

    to_drop=pd.Series(join_directs_nulls["auxiliar_index"].values).unique()
    df_kayak=df_kayak.drop(axis=0,index=to_drop.tolist())

#######################################################################

    join_scale_numbers["flightNumber_v1_py"]=join_scale_numbers["flightNumber_v1"]
    join_scale_numbers["flightNumber_v2_py"]=join_scale_numbers["flightNumber_v2"]

    join_directs_numbers["flightNumber_v1_py"]=join_directs_numbers["flightNumber_v1"]
    join_directs_numbers["flightNumber_v2_py"]=join_directs_numbers["flightNumber_v2"]

    join_directs_nulls["flightNumber_v2_py"]=join_directs_nulls["flightNumber_v2"]

    joined=pd.concat([join_directs_nulls,join_directs_numbers,join_scale_numbers,join_scale_nulls],sort=True)
    joined=joined.reset_index().drop(axis=1,columns=["auxiliar_index","index"])


################################ Add rating columns#############################################

    ratings = ["ontime_v1", "late15_v1", "late30_v1", "late45_v1", "delayMean_v1", "delayStandardDeviation_v1", "delayMin_v1", "delayMax_v1", "allOntimeCumulative_v1",
               "allOntimeStars_v1", "allDelayCumulative_v1", "allDelayStars_v1", "ontime_v2", "late15_v2", "late30_v2",
               "late45_v2", "delayMean_v2", "delayStandardDeviation_v2", "delayMin_v2", "delayMax_v2", "allOntimeCumulative_v2",
               "allOntimeStars_v2", "allDelayCumulative_v2", "allDelayStars_v2"]

    dic = {}
    for rating in ratings:
        dic[rating] = np.full(len(joined), "null")
#         dic[rating] = np.ones(len(joined))

    dummies = pd.DataFrame(dic)
    final_joined = pd.merge(left=joined, right=dummies, left_index=True, right_index=True)




  ############################################################################

    getRatings(final_joined)

  ######### ordering of columns

    cols_df_part1 = df.columns.tolist()[0:23]
    ratings_v1 = ratings[0:12]
    cols_df_part2=df.columns.tolist()[23:]
    ratings_v2=ratings[12:]
    python_columns=["flightNumber_v1_py","flightNumber_v2_py","precio","duracion","enlaces"]
    final_joined=final_joined[cols_df_part1+ratings_v1+cols_df_part2+ratings_v2+python_columns]
    logger.info("From final CSV : {}".format(len(final_joined)))


    ######### eliminar duplicados por href

    to_keep = final_joined["enlaces"].drop_duplicates().index.tolist()
    final_joined = final_joined.loc[to_keep][:].reset_index(drop=True)

    logger.info("from which, only stayed : {}".format(len(final_joined)))

    ###############################

    final_joined["flightNumber_v1"] = final_joined["flightNumber_v1"].astype(str)
    final_joined["flightNumber_v2"] = final_joined["flightNumber_v2"].astype(str)
    final_joined["flightNumber_v1_py"] = final_joined["flightNumber_v1_py"].astype(str)
    final_joined["flightNumber_v2_py"] = final_joined["flightNumber_v2_py"].astype(str)



    final_joined.to_csv(path_final_routes+"/"+departure+"_"+arrival+"_"+date+".csv",sep=";",na_rep="null",index=False)




# setea en df según escala
def setRating(df, ix, isFirstScale, field, doc):
    if isFirstScale:
        key = key = field+"_v1"
    else:
        key = key = field+"_v2"
    df.at[ix, key] = doc[field]
                
# actualiza ratings del df                
def putDBRating(df, db, ix, id, isFirstScale):
#     logger.info("id [" + id + "]") 
#     doc = db.routeRatings.find_one({'_id': 'AAQ-DME-EK-7802'})
    doc = db.routeRatings.find_one({'_id': id})
    # check if the find_one() call returns 'None'
    if doc == None:
        logger.warn("No ratings for id [" + id + "]") 
    else:
        try:
            setRating(df, ix, isFirstScale, "ontime", doc)
            setRating(df, ix, isFirstScale, "late15", doc)
            setRating(df, ix, isFirstScale, "late30", doc)
            setRating(df, ix, isFirstScale, "late45", doc)
            setRating(df, ix, isFirstScale, "delayMean", doc)
            setRating(df, ix, isFirstScale, "delayStandardDeviation", doc)
            setRating(df, ix, isFirstScale, "delayMin", doc)
            setRating(df, ix, isFirstScale, "delayMax", doc)
            setRating(df, ix, isFirstScale, "allOntimeCumulative", doc)
            setRating(df, ix, isFirstScale, "allOntimeStars", doc)
            setRating(df, ix, isFirstScale, "allDelayCumulative", doc)
            setRating(df, ix, isFirstScale, "allDelayStars", doc)
        except KeyError as err:
            logger.error("Error getting ratings [" + err + "] for id [" + id + "]") 


def getRatings(df):
    
    #acceso collection ratings
    client = MongoClient()
    db = client.historicalData
    for ix, row in df.iterrows():
#         print (row)
#         print (type(row))
#         print (row["departureAirportFsCode_v1"])
        id_v1 = row["departureAirportFsCode_v1"] + "-" + row["arrivalAirportFsCode_v1"] + "-" + \
                row["carrierFsCode_v1"] + "-" + str(row["flightNumber_v1_py"])
        putDBRating(df, db, ix, id_v1, True)
        if row["directo"] == False:
            id_v2 = row["departureAirportFsCode_v2"] + "-" + row["arrivalAirportFsCode_v2"] + "-" + \
                    row["carrierFsCode_v2"] + "-" + str(row["flightNumber_v2_py"])
            putDBRating(df, db, ix, id_v2, False)
        
        

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
    logger.info("JOIN process starting from [" + cmdargs + "]")
    doJoin(sys.argv[1],sys.argv[2],sys.argv[3])
    logger.info("JOIN process finished.")
    logger.info("------------------------------------------------------------------------------------------------------------------------")












