import pandas as pd
import requests

STATION = "USW00094728"  # NY CITY CENTRAL PARK, NY US
START = "2022-02-01"
END   = "2022-02-28"

# Pull NOAA/NCEI daily-summaries as JSON (metric units)
url = "https://www.ncei.noaa.gov/access/services/data/v1"
params = {
    "dataset": "daily-summaries",
    "stations": STATION,
    "startDate": START,
    "endDate": END,
    "format": "json",
    "units": "metric",
    "includeAttributes": "false",
    "includeStationName": "false",
    "dataTypes": "TMIN,TMAX,PRCP,SNOW,SNWD"
}

r = requests.get(url, params=params, timeout=60)
r.raise_for_status()
data = r.json()

df = pd.DataFrame(data)
# NCEI typically returns DATE plus requested datatypes (as strings)
df["date"] = pd.to_datetime(df["DATE"]).dt.date

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

df["tmin_c"] = to_num(df.get("TMIN"))
df["tmax_c"] = to_num(df.get("TMAX"))
df["prcp_mm"] = to_num(df.get("PRCP"))
df["snow_mm"] = to_num(df.get("SNOW"))
df["snwd_mm"] = to_num(df.get("SNWD"))

df["tmin_f"] = df["tmin_c"] * 9/5 + 32
df["tmax_f"] = df["tmax_c"] * 9/5 + 32

df["prcp_in"] = df["prcp_mm"] / 25.4
df["snow_in"] = df["snow_mm"] / 25.4
df["snwd_in"] = df["snwd_mm"] / 25.4

out = df[[
    "date",
    "tmin_c","tmax_c",
    "tmin_f","tmax_f",
    "prcp_mm","prcp_in",
    "snow_mm","snow_in",
    "snwd_mm","snwd_in"
]].sort_values("date")

out.to_csv("central_park_weather_2022_02.csv", index=False)
print("Wrote: central_park_weather_2022_02.csv")

