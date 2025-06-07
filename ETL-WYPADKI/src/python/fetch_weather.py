from datetime import datetime, timedelta
import pandas as pd
from meteostat import Point, Daily
import warnings
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine

# Połączenie do SQL Server
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
        df_weather = Daily(point, start, end).fetch()

        # Pełna lista dat do pokrycia – niezależnie od danych
        full_dates = set((start + timedelta(days=i)).date() for i in range((end - start).days + 1))
        available_dates = set()

        if df_weather is not None and not df_weather.empty:
            df_weather = df_weather.reset_index()
            df_weather['time'] = pd.to_datetime(df_weather['time']).dt.date
            available_dates = set(df_weather['time'])

            for _, wd in df_weather.iterrows():
                weather_data.append({
                    'weather_date': wd['time'],
                    'longitude': lon,
                    'latitude': lat,
                    'tavg': wd.get('tavg', 0) or 0,
                    'tmin': wd.get('tmin', 0) or 0,
                    'tmax': wd.get('tmax', 0) or 0,
                    'prcp': wd.get('prcp', 0) or 0,
                    'snow': wd.get('snow', 0) or 0,
                    'wdir': wd.get('wdir', 0) or 0,
                    'wspd': wd.get('wspd', 0) or 0,
                    'wpgt': wd.get('wpgt', 0) or 0,
                    'pres': wd.get('pres', 0) or 0,
                    'tsun': wd.get('tsun', 0) or 0
                })
        else:
            print(f"⚠ Brak danych pogodowych dla ({lat}, {lon}) w zakresie {start.date()} - {end.date()}")

        # Dodaj brakujące daty zerami
        missing_dates = full_dates - available_dates
        for missing in missing_dates:
            weather_data.append({
                'weather_date': missing,
                'longitude': lon,
                'latitude': lat,
                'tavg': 0,
                'tmin': 0,
                'tmax': 0,
                'prcp': 0,
                'snow': 0,
                'wdir': 0,
                'wspd': 0,
                'wpgt': 0,
                'pres': 0,
                'tsun': 0
            })

        print(f"✔ Obsłużono lokalizację {idx + 1}/{total} ({lat}, {lon})")

    except Exception as e:
        print(f"⚠ Błąd dla lokalizacji ({lat}, {lon}): {e}")

# Wstawienie do bazy
if weather_data:
    df_result = pd.DataFrame(weather_data)
    df_result.to_sql('Stg_Weather', con=engine, if_exists='append', index=False)
    print(f"\n✅ Załadowano {len(df_result)} rekordów (w tym brakujące z zerami) do tabeli Stg_Weather.")
else:
    print("⚠ Brak danych do załadowania.")

