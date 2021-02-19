import pandas as pd
from urllib.request import urlopen, urlretrieve
import datetime

states = [
  "2/3/2021 10:11 AM     11235544 la.data.10.Arkansas",
  "2/3/2021 10:11 AM     33346152 la.data.11.California",
  "2/3/2021 10:11 AM     11296520 la.data.12.Colorado",
  "2/3/2021 10:11 AM     17295928 la.data.13.Connecticut",
  "2/3/2021 10:11 AM      1376008 la.data.14.Delaware",
  "2/3/2021 10:11 AM       933788 la.data.15.DC",
  "2/3/2021 10:11 AM     17573420 la.data.16.Florida",
  "2/3/2021 10:11 AM     21407304 la.data.17.Georgia",
  "2/3/2021 10:11 AM      1199120 la.data.18.Hawaii",
  "2/3/2021 10:11 AM      7024552 la.data.19.Idaho",
  "2/3/2021 10:11 AM     24168224 la.data.20.Illinois",
  "2/3/2021 10:11 AM     15888324 la.data.21.Indiana",
  "2/3/2021 10:11 AM     14271024 la.data.22.Iowa",
  "2/3/2021 10:11 AM     13261176 la.data.23.Kansas",
  "2/3/2021 10:11 AM     15283948 la.data.24.Kentucky",
  "2/3/2021 10:11 AM      9484736 la.data.25.Louisiana",
  "2/3/2021 10:11 AM     50530636 la.data.26.Maine",
  "2/3/2021 10:11 AM      4362868 la.data.27.Maryland",
  "2/3/2021 10:11 AM     35515392 la.data.28.Massachusetts",
  "2/3/2021 10:11 AM     19589772 la.data.29.Michigan",
  "2/3/2021 10:11 AM     14749992 la.data.30.Minnesota",
  "2/3/2021 10:11 AM     11649164 la.data.31.Mississippi",
  "2/3/2021 10:11 AM     16930064 la.data.32.Missouri",
  "2/3/2021 10:11 AM      6682648 la.data.33.Montana",
  "2/3/2021 10:11 AM     10696732 la.data.34.Nebraska",
  "2/3/2021 10:11 AM      3410220 la.data.35.Nevada",
  "2/3/2021 10:11 AM     25592072 la.data.36.NewHampshire",
  "2/3/2021 10:11 AM     11622344 la.data.37.NewJersey",
  "2/3/2021 10:11 AM      6063540 la.data.38.NewMexico",
  "2/3/2021 10:12 AM     18296320 la.data.39.NewYork",
  "2/3/2021 10:12 AM     18014756 la.data.40.NorthCarolina",
  "2/3/2021 10:12 AM      6568248 la.data.41.NorthDakota",
  "2/3/2021 10:12 AM     20142236 la.data.42.Ohio",
  "2/3/2021 10:12 AM     11685696 la.data.43.Oklahoma",
  "2/3/2021 10:12 AM      8242316 la.data.44.Oregon",
  "2/3/2021 10:12 AM     16124840 la.data.45.Pennsylvania",
  "2/3/2021 10:12 AM      8473784 la.data.46.PuertoRico",
  "2/3/2021 10:12 AM      4559992 la.data.47.RhodeIsland",
  "2/3/2021 10:12 AM      8396108 la.data.48.SouthCarolina",
  "2/3/2021 10:12 AM      7920864 la.data.49.SouthDakota",
  "2/3/2021 10:12 AM     15143808 la.data.50.Tennessee",
  "2/3/2021 10:12 AM     45956744 la.data.51.Texas",
  "2/3/2021 10:12 AM      6374900 la.data.52.Utah",
  "2/3/2021 10:12 AM     25529584 la.data.53.Vermont",
  "2/3/2021 10:12 AM     17895016 la.data.54.Virginia",
  "2/3/2021 10:12 AM      9977328 la.data.56.Washington",
  "2/3/2021 10:12 AM      8097752 la.data.57.WestVirginia",
  "2/3/2021 10:12 AM     13203760 la.data.58.Wisconsin",
  "2/3/2021 10:12 AM      3646952 la.data.59.Wyoming",
  "2/3/2021 10:12 AM     12493132 la.data.7.Alabama",
  "2/3/2021 10:12 AM      3580956 la.data.8.Alaska",
  "2/3/2021 10:12 AM      5343036 la.data.9.Arizona"
]
states = [s[31:] for s in states]

def clean_df(df, year_since):
    year = []
    period = []
    unemployment = []
    
    series_id = df[df.columns[0]][0][:20]
    
    for point in df[df.columns[0]]: #30 years from 1990, 12 periods per year. This is to filter other data that appears after the unemployment
        _ = point.split("\t")
        if _[0].strip() == series_id and int(_[1])>=year_since:
            year.append(int(_[1]))
            period.append(_[2])
            try:
                unemployment.append(float(_[3]))
            except:
                unemployment.append(float('NaN'))
            
        else:
            pass
    
    df = pd.DataFrame({
        
        'year':year,
        'period':period,
        'unemployment':unemployment
    })
    
    return df.groupby('year', as_index=False)['unemployment'].mean()

def download_state_file(base_url, state):
    r = urlopen(base_url+state)
    f = open(f'{state} unemployment data.txt', 'w')
    f.write(r.read().decode('utf-8'))
    f.close()

def produce_csv_state(year_since):
    state_level_unemployment = pd.DataFrame()

    for state in states:
        download_state_file('https://download.bls.gov/pub/time.series/la/', state)
        df = pd.read_csv(f'{state} unemployment data.txt')
        df = clean_df(df, year_since)
        if state[10:][0] == '.':
            name = state[11:]
        else:
            name = state[10:]
        df["state"] = name
        #state_level_unemployment = state_level_unemployment + generate_df(df)
        state_level_unemployment = pd.concat([state_level_unemployment, df], ignore_index=True, sort=False)
        print(state, "done")
    
    state_level_unemployment.to_csv(f'States Unemployment since {year_since}.csv')


####################################################################################################################

current_year = datetime.datetime.now().year
years_to_parse = [str(y)[-2:] for y in range(1990, current_year+1)]

def download_file_county(year):
    urlretrieve(f"https://www.bls.gov/lau/laucnty{year}.xlsx", f"{year} unemp county level data.xlsx")

def produce_csv_county():
    county_level_unemployment = pd.DataFrame()

    for year in years_to_parse:
        try:
            download_file_county(year)
            df = pd.read_excel(f"{year} unemp county level data.xlsx", sheet_name=f"laucnty{year}",usecols ="D,E,J", skiprows=[0,1,2,3,4])
            df.dropna(inplace=True)
            county_level_unemployment = pd.concat([county_level_unemployment, df], ignore_index=True, sort=False)
            print(f'year {year} for county data done')
            #county_level_unemployment = county_level_unemployment + df
        except:
            pass

    county_level_unemployment.rename(columns={"Unnamed: 3": "county", "Unnamed: 4": "year", "Unnamed: 9": "unemployment"}, inplace=True)
    county_level_unemployment = county_level_unemployment.astype({"year": int})

    county_level_unemployment.to_csv('County level unemployment data.csv')


####################################################################################################################

produce_csv_county()
produce_csv_state(1990)