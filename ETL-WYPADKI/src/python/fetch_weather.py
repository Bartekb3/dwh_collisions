a = '''
import warnings
warnings.filterwarnings("ignore")  # Wyciszenie wszystkich warningów, np. FutureWarning z meteostat/pandas

from meteostat import Hourly, Point
from datetime import datetime
import pandas as pd
import pyodbc

# Połączenie do SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DWH_Staging;Trusted_Connection=yes'
)
cursor = conn.cursor()

# Pobranie unikalnych kombinacji lokalizacji i godziny
query = """
SELECT DISTINCT
    ROUND(longitude, 1) AS lon,
    ROUND(latitude, 1) AS lat,
    CAST(CONCAT([date], ' ', LEFT(accident_time, 2), ':00') AS DATETIME) AS dt
FROM Stg_Collisions_Enriched
WHERE accident_time IS NOT NULL
"""

df_keys = pd.read_sql(query, conn)

# Lista na dane pogodowe
weather_rows = []
total = len(df_keys)

for i, row in df_keys.iterrows():
    lon, lat, dt = row['lon'], row['lat'], row['dt']
    try:
        point = Point(lat, lon)
        data = Hourly(point, dt, dt).fetch()

        if not data.empty:
            wd = data.iloc[0]
            weather_rows.append({
                'weather_datetime': dt,
                'longitude': lon,
                'latitude': lat,
                'temp': wd.get('temp'),
                'dwpt': wd.get('dwpt'),
                'rhum': wd.get('rhum'),
                'prcp': wd.get('prcp'),
                'snow': wd.get('snow'),
                'wdir': wd.get('wdir'),
                'wspd': wd.get('wspd'),
                'wpgt': wd.get('wpgt'),
                'pres': wd.get('pres'),
                'tsun': wd.get('tsun'),
                'coco': wd.get('coco'),
            })
    except Exception as e:
        print(f"Błąd: ({lat}, {lon}, {dt}): {e}")

    # Log co 100 wierszy
    if (i + 1) % 100 == 0:
        print(f"Pobrano pogodę dla {i + 1} z {total} lokalizacji...")

# Wstawianie do tabeli
df_weather = pd.DataFrame(weather_rows)

if not df_weather.empty:
    for _, row in df_weather.iterrows():
        cursor.execute("""
        INSERT INTO Stg_Weather (
            weather_datetime, longitude, latitude,
            temp, dwpt, rhum, prcp, snow,
            wdir, wspd, wpgt, pres, tsun, coco
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.weather_datetime, row.longitude, row.latitude,
             row.temp, row.dwpt, row.rhum, row.prcp, row.snow,
             row.wdir, row.wspd, row.wpgt, row.pres, row.tsun, row.coco)

    conn.commit()
    print(f"✔ Załadowano {len(df_weather)} wierszy do Stg_Weather.")
else:
    print("⚠ Brak danych pogodowych do załadowania.")

# Zamknięcie połączenia
cursor.close()
conn.close()

from meteostat import Hourly, Point
from datetime import datetime
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import time
import warnings
warnings.filterwarnings("ignore")  # Wyciszenie wszystkich warningów, np. FutureWarning z meteostat/pandas



# Połączenie z bazą danych
conn_str = (
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=localhost;"
    "DATABASE=DWH_Staging;"
    "Trusted_Connection=yes;"
)

# URI kodowany
conn_uri = "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)

# Tworzymy silnik
engine = create_engine(conn_uri)
# Pobranie unikalnych lokalizacji z zakresem czasowym
query = """
SELECT
    ROUND(longitude, 1) AS lon,
    ROUND(latitude, 1) AS lat,
    MIN(CAST(CONCAT([date], ' ', LEFT(accident_time, 2), ':00') AS DATETIME)) AS dt_start,
    MAX(CAST(CONCAT([date], ' ', LEFT(accident_time, 2), ':00') AS DATETIME)) AS dt_end
FROM Stg_Collisions_Enriched
WHERE accident_time IS NOT NULL
GROUP BY ROUND(longitude, 1), ROUND(latitude, 1)

"""

df_locations = pd.read_sql(query, engine)

# Lista wyników
weather_rows = []

# Przetwarzanie lokalizacji
for i, row in df_locations.iterrows():
    lon, lat = row['lon'], row['lat']
    dt_start, dt_end = row['dt_start'], row['dt_end']

    try:
        point = Point(lat, lon)
        data = Hourly(point, dt_start, dt_end).fetch()

        if not data.empty:
            data = data.reset_index()
            data['longitude'] = lon
            data['latitude'] = lat
            data.rename(columns={'time': 'weather_datetime'}, inplace=True)
            weather_rows.extend(data.to_dict(orient='records'))

        print(f"✔ Lokalizacja {i + 1} z {len(df_locations)} pobrana")

        time.sleep(1)  # unikanie throttlingu API

    except Exception as e:
        print(f"⚠ Błąd dla lokalizacji ({lat}, {lon}): {e}")

# Konwersja i zapis do SQL
if weather_rows:
    df_weather = pd.DataFrame(weather_rows)[[
        'weather_datetime', 'longitude', 'latitude',
        'temp', 'dwpt', 'rhum', 'prcp', 'snow',
        'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'coco'
    ]]

    df_weather.to_sql('Stg_Weather', engine, if_exists='append', index=False)
    print("✅ Dane pogodowe zapisane do Stg_Weather")

else:
    print("ℹ Brak danych pogodowych do zapisania.")
'''

from datetime import datetime
import pandas as pd
from meteostat import Point, Daily
import warnings
warnings.filterwarnings("ignore")  # Wyciszenie wszystkich warningów, np. FutureWarning z meteostat/pandas
from sqlalchemy import create_engine


# Dane połączenia z SQL Server
server = 'localhost'
database = 'DWH_Staging'
conn_str = (
    f"mssql+pyodbc://{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = create_engine(conn_str)

# Pobranie unikalnych lokalizacji i zakresów dat
query = """
SELECT
    ROUND(longitude, 1) AS lon,
    ROUND(latitude, 1) AS lat,
    MIN(CAST([date] AS DATETIME)) AS dt_start,
    MAX(CAST([date] AS DATETIME)) AS dt_end
FROM Stg_Collisions_Enriched
WHERE longitude IS NOT NULL AND latitude IS NOT NULL AND [date] IS NOT NULL
GROUP BY ROUND(longitude, 1), ROUND(latitude, 1)
"""

df_locations = pd.read_sql(query, engine)
weather_data = []

total = len(df_locations)

for idx, row in df_locations.iterrows():
    lon, lat, start, end = row['lon'], row['lat'], row['dt_start'], row['dt_end']
    try:
        point = Point(lat, lon)
        df_weather = Daily(point, start, end).fetch().reset_index()

        for _, wd in df_weather.iterrows():
            weather_data.append({
                'weather_date': wd['time'].date(),
                'longitude': lon,
                'latitude': lat,
                'tavg': wd.get('tavg'),
                'tmin': wd.get('tmin'),
                'tmax': wd.get('tmax'),
                'prcp': wd.get('prcp'),
                'snow': wd.get('snow'),
                'wdir': wd.get('wdir'),
                'wspd': wd.get('wspd'),
                'wpgt': wd.get('wpgt'),
                'pres': wd.get('pres'),
                'tsun': wd.get('tsun')
            })

        print(f"✔ Pobrano dane: {idx + 1}/{total}")

    except Exception as e:
        print(f"⚠ Błąd dla lokalizacji ({lat}, {lon}): {e}")

# Wstawienie do bazy
if weather_data:
    df_result = pd.DataFrame(weather_data)
    df_result.to_sql('Stg_Weather', con=engine, if_exists='append', index=False)
    print(f"\n✅ Załadowano {len(df_result)} rekordów do tabeli Stg_Weather.")
else:
    print("⚠ Brak danych do załadowania.")
