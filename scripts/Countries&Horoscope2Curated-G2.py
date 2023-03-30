import sys
import pandas as pd

#DATASETS TREATMENT FROM RAW TO CURATED

#COUNTRIESBYCONTINENT DATASET

countriesSource = "s3://pfinal-p2-grupo2/raw/countryContinent.csv"
contriesCurated = "s3://pfinal-p2-grupo2/curated/countryContinentCured.csv"

dtf_countries = pd.read_csv(countriesSource, encoding_errors='replace')

dtf_countries = dtf_countries[["country", "continent"]]

if "Korea (Democratic People's Republic of)" in dtf_countries['country'].values:
    dtf_countries.loc[dtf_countries['country'] == "Korea (Democratic People's Republic of)", 'country'] = 'Democratic Republic of Korea'

dtf_countries.to_csv(contriesCurated, index=False)

#HOROSCOPE DATASET

horoscopeSource = "s3://pfinal-p2-grupo2/raw/horoscope.csv"
horoscopeCurated = "s3://pfinal-p2-grupo2/curated/horoscopeCured.csv"

dtf_horoscope = pd.read_csv(horoscopeSource)

new_col = []
mes = 0
new_int = 0

#Date transformation to int to facilitate join in the next steps

for x in dtf_horoscope["TimeStart"]:
    mes += 100
    fecha = x.split("-")
    new_int = mes + int(fecha[0])
    if new_int < 120: mes = 0
    new_col.append(new_int)

dtf_horoscope["TimeStartInt"] = new_col
new_col=[]
mes=0

for x in dtf_horoscope["TimeEnd"]:
    mes += 100
    fecha = x.split("-")
    new_int = mes + int(fecha[0])
    if new_int > 1300: new_int = new_int - 100
    new_col.append(new_int)
    
dtf_horoscope["TimeEndInt"] = new_col

dtf_horoscope.to_csv(horoscopeCurated, index=False)