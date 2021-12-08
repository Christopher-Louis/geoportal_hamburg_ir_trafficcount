import pandas as pd
import datetime as dt
#from datetime import datetime
import json
#from pandas.io.json import json_normalize
import requests

def iot_source_def():
    id_querystring1 = "https://iot.hamburg.de/v1.1/Things?%24filter=Datastreams%2Fproperties%2FserviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27&$count=true&$expand=Locations,Datastreams($expand=Observations($top=3;$orderby=phenomenonTime%20desc),Sensor,ObservedProperty)"
    id_querystring2 = "https://iot.hamburg.de/v1.1/Things?$skip=100&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_HamburgerRadzaehlnetz%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring3 = "https://iot.hamburg.de/v1.1/Things?$skip=200&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_HamburgerRadzaehlnetz%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring4 = "https://iot.hamburg.de/v1.1/Things?$skip=300&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring5 = "https://iot.hamburg.de/v1.1/Things?$skip=400&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring6 = "https://iot.hamburg.de/v1.1/Things?$skip=500&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring7 = "https://iot.hamburg.de/v1.1/Things?$skip=600&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring8 = "https://iot.hamburg.de/v1.1/Things?%24filter=Datastreams%2Fproperties%2FserviceName%20eq%20%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27&$count=true&$expand=Locations,Datastreams($expand=Observations($top=3;$orderby=phenomenonTime%20desc),Sensor,ObservedProperty)"
    id_querystring9 = "https://iot.hamburg.de/v1.1/Things?$skip=100&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    id_querystring10 = "https://iot.hamburg.de/v1.1/Things?$skip=200&$filter=%28Datastreams%2Fproperties%2FserviceName+eq+%27HH_STA_AutomatisierteVerkehrsmengenerfassung%27%29&$expand=Locations,Datastreams%28%24expand%3DObservations%28%24top%3D3%3B%24orderby%3DphenomenonTime+desc%29%2CSensor%2CObservedProperty%29&$count=true"
    return [id_querystring1, id_querystring2, id_querystring3, id_querystring4, id_querystring5, id_querystring6, id_querystring7, id_querystring8, id_querystring9, id_querystring10]


def extract_station_data(search_string, type, id_df):
    new_df = pd.DataFrame()
    new_df['iot_id'] = None
    new_df['coordinates'] = None
    new_df["observedArea.type"] = None
    new_df["observation_type"] = None
    new_df["lat"] = None
    new_df["lon"] = None
    i = 0

    for x in id_df.index:
        if id_df["properties.layerName"][x] == search_string:

            if id_df["observedArea.coordinates"][x] != None:
                
                new_df.at[i, "observation_type"] = type
                new_df.at[i, "iot_id"] = id_df["@iot.id"][x]
                new_df.at[i, "coordinates"] = id_df["observedArea.coordinates"][x]
                new_df.at[i, "observedArea.type"] = id_df["observedArea.type"][x]

                if id_df["observedArea.type"][x] == "Point":
                    new_df.at[i, "lat"] = new_df.at[i, "coordinates"][1]
                    new_df.at[i, "lon"] = new_df.at[i, "coordinates"][0]
                
                    i += 1

                if id_df["observedArea.type"][x] == "LineString":
                    new_df.at[i, "lat"] = new_df.at[i, "coordinates"][0][1]
                    new_df.at[i, "lon"] = new_df.at[i, "coordinates"][0][0]

                    i += 1
    
    new_df.drop('coordinates', axis=1, inplace=True)
    new_df.drop('observedArea.type', axis=1, inplace=True)

    return new_df


def download_id_list(list):
    id_df = pd.DataFrame()
    id_df_master = pd.DataFrame()
    search_bike = "Anzahl_Fahrraeder_Zaehlstelle_1-Tag"
    search_car = "Anzahl_Kfz_Zaehlstelle_1-Tag"
    for id in list:
        json_object = get_iotdata(id)
        print(id)
        id_df = extract_station_data(search_bike, "bike", pd.json_normalize(json_object, record_path=["value", "Datastreams"]))
        id_df_master = id_df_master.append(id_df, ignore_index=True)
        id_df = extract_station_data(search_car, "car", pd.json_normalize(json_object, record_path=["value", "Datastreams"]))
        id_df_master = id_df_master.append(id_df, ignore_index=True)
    return id_df_master

def deduct_days(date, n_days):
    result = date - dt.timedelta(days=n_days)
    return result

def download_for_one_iot_id(id):
    df_stationdownload = pd.DataFrame()
    today = dt.date.today()
    tomorrow = today + dt.timedelta(days=1)
    json_not_empty = True
    i = 1

    while json_not_empty:

        id_iot = id
        skip = i 
        start = deduct_days(tomorrow,i)
        start = start.strftime("%Y-%m-%d")
        end = deduct_days(tomorrow,i+97)
        end = end.strftime("%Y-%m-%d")
        
        querystring = f"https://iot.hamburg.de/v1.1/Datastreams({id_iot})/Observations?$skip=0&$filter=%28%28phenomenonTime+ge+{end}T23%3A00%3A00.000Z%29+and+%28phenomenonTime+lt+{start}T23%3A00%3A00.000Z%29%29"

        json_object = get_iotdata(querystring)
        data_df = pd.json_normalize(json_object, record_path="value")
    
        df_stationdownload["iot_id"] = id_iot
        
        if json_object == {'value': []}:
            
            return df_stationdownload
            json_not_empty = False

        if i > 1200:
            return df_stationdownload
            json_not_empty = False
            print("safety break")
        df_stationdownload = pd.concat([df_stationdownload, data_df], ignore_index=True)
        i+=95

def get_iotdata(q):
    querystring = q
    iotdata_response = requests.request("GET", querystring)
    result = json.loads(iotdata_response.content)
    return result

def mass_download(df):
    return_df = pd.DataFrame()
    for id in df.iot_id:
        df_one_id = download_for_one_iot_id(id)
        df_one_id = remove_duplicates(df_one_id, "phenomenonTime")
        return_df = return_df.append(df_one_id)
        
    return return_df
    
def remove_duplicates(df, column):
    df.drop_duplicates(subset=column, keep="last", inplace=True)
    return df

def cleaning_infrared(df):
    df["date"] = df["phenomenonTime"].str[25:35]
    df.drop('@iot.selfLink', axis=1, inplace=True)
    df.drop('@iot.id', axis=1, inplace=True)    
    df.drop('Datastream@iot.navigationLink', axis=1, inplace=True)
    df.drop('resultTime', axis=1, inplace=True)
    df.drop('MultiDatastream@iot.navigationLink', axis=1, inplace=True)	
    df.drop('FeatureOfInterest@iot.navigationLink', axis=1, inplace=True)
    return df

def main():
    print("Scraping links for iot IDs:")
    id_source_list = iot_source_def()
    id_df = download_id_list(id_source_list)
    id_df.to_csv ('station_ids.csv', index = False, header=True)
    print("IDs successfully downloaded. Downloading available Data.")
    all_station_df = mass_download(id_df)
    all_station_df = cleaning_infrared(all_station_df)
    print("Mass download success. Storing in CSV files.")
    
    all_station_df.to_csv ('station_data.csv', index = False, header=True)


if __name__ == "__main__":
    main()