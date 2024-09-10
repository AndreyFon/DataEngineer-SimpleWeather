import argparse
import requests
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup

#Using args to run certain function when using DockerOperator in Airflow
def main(args):
    functions_dict = {"api_get_weather_now": api_get_weather_now,
                      "api_get_weather_day": api_get_weather_day,
                      "api_get_places": api_get_places
    }

    if(args[0] in functions_dict.keys()):
        if(len(args)>1):
            functions_dict[args[0]](args[1:])
        else:
            functions_dict[args[0]]()

#get USA states capitals data, latitude/longitude
def api_get_places():
    #scraping names from wiki
    scraping_url = "https://simple.wikipedia.org/wiki/List_of_U.S._state_capitals"
    scrap_r = requests.get(scraping_url)
    soup = BeautifulSoup(scrap_r.content, "html.parser")

    state_list = []
    capital_list = []
    flag = 0
    
    for row in soup.find_all("a"):
        if(row.parent.name == "td"):
            if(flag%2 == 0):
                state_list.append(row.text)
            else:
                capital_list.append(row.text)
            flag += 1

    #engine to connect to postgres database
    engine = create_engine('postgresql://admin:admin@localDB:5432/weather')
    
    api_url = "https://geocoding-api.open-meteo.com/v1/search"

    for state, capital in zip(state_list, capital_list):
        api_params = dict(
            name=capital,
            count=5,
            language="en",
            format="json"
        )
        r = requests.get(url=api_url, params=api_params)
        
        #data of interest from the recieved api call
        keys_of_interest = ["latitude", "longitude", "country", "timezone"]

        #iterate over couple results and get only USA places. In case first returned data just have simmilar name
        for i in r.json()["results"]:
            if(i["country"]=="United States"):
                tmp = r.json()
                relevant_data = {key: i[key] for key in keys_of_interest}
                break

        df_data = pd.DataFrame({"state": state, "capital": capital, **relevant_data}, index=[0])
        df_data.to_sql(name="weather_api_places", con=engine, if_exists="append", index=False)

    #Closing Engine     
    engine.dispose()

#gets current weather data
def api_get_weather_now():
    engine = create_engine('postgresql://admin:admin@localDB:5432/weather')
    
    api_url = "https://api.open-meteo.com/v1/forecast"
    
    #check if table with places exist/can connect 
    try:
        places_data = pd.read_sql(con=engine, sql="weather_api_places")
        
        tmp_list = []

        #get current weather data per place in places table from DB
        for row in places_data.itertuples():
            api_params = dict(
            latitude=row.latitude,    
            longitude=row.longitude,
            current=["temperature_2m","relative_humidity_2m","apparent_temperature","is_day",
            "precipitation","weather_code","cloud_cover","wind_speed_10m"]
            )
            
            r = requests.get(url=api_url, params=api_params)
            tmp_list.append({"capital": row.capital, **r.json()["current"]})
        
        #replace old data for current weather with the new data
        df_data = pd.DataFrame(tmp_list)
        df_data.to_sql(name="weather_api_now", con=engine, if_exists="replace", index=False)

    #NOTE: BAD PRACTICE CAPTURING ALL EXCEPTIONS/ERRORS
    except:
        print("---Could not connect to DB or table does not exist---")    

    #Closing Engine     
    engine.dispose()

#gets weather data of certain day. Day must be in 'yyyy-mm-dd' format
def api_get_weather_day(day):
    day = day[0]
    engine = create_engine('postgresql://admin:admin@localDB:5432/weather')
    api_url = "https://api.open-meteo.com/v1/forecast"
    
    #checking for existence of places table
    try:
        places_data = pd.read_sql(con=engine, sql="weather_api_places")
        
        tmp_list = []

        #get for specific day weather data per place in places table from DB
        for row in places_data.itertuples():
            api_params = dict(
            latitude=row.latitude, longitude=row.longitude,
            hourly=["temperature_2m","relative_humidity_2m","apparent_temperature","precipitation_probability","cloud_cover","wind_speed_10m"],
            start_date=day, end_date=day
            )

            r = requests.get(url=api_url, params=api_params)

            for tpl in zip(r.json()["hourly"]["time"], r.json()["hourly"]["temperature_2m"], r.json()["hourly"]["relative_humidity_2m"],r.json()["hourly"]["apparent_temperature"],
                           r.json()["hourly"]["precipitation_probability"], r.json()["hourly"]["cloud_cover"], r.json()["hourly"]["wind_speed_10m"]):
                tmp_list.append([row.capital, *tpl])
       
        df_data = pd.DataFrame(tmp_list, columns=["capital", "time", *api_params["hourly"]])
        
        #creates table for specific day. EX: for day "2020-02-20" table name will be: "weather_api_day_2020_02_20". replaces old data if table with same name exist
        df_data.to_sql(name="weather_api_day_"+day.replace("-", "_"), con=engine, if_exists="replace", index=False)

    except:
        print("---Could not connect to DB or table does not exist---")    

    #Closing Engine     
    engine.dispose()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help="Name of function to call with its parameters", nargs="*")
    args = parser.parse_args()

    main(args.f)
