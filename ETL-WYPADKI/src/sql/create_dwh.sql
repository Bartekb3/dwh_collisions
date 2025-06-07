IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DWH_RoadSafety')
BEGIN
    CREATE DATABASE DWH_RoadSafety;
END;
GO

USE DWH_RoadSafety;
GO

IF OBJECT_ID('dbo.dim_time', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_time (
        time_sk INT IDENTITY(1,1) PRIMARY KEY,         -- Surrogate Key
        time_id VARCHAR(20) NOT NULL UNIQUE,           -- Natural Key (yyyyMMddHH)
        date DATE NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        month_name VARCHAR(20) NOT NULL,
        day INT NOT NULL,
        day_of_week VARCHAR(20) NOT NULL,
        day_of_week_num INT NOT NULL,
        is_weekend BIT NOT NULL,
        week_number INT NOT NULL,
        quarter INT NOT NULL,
        hour INT NOT NULL,
        hour_band VARCHAR(20) NOT NULL,
        part_of_day VARCHAR(20) NOT NULL,
        is_night BIT NOT NULL,
        is_rush_hour BIT NOT NULL,
        is_holiday BIT NOT NULL DEFAULT (0)            -- Mo¿na póŸniej aktualizowaæ na podstawie kalendarza œwi¹t
    );
END;
GO

IF OBJECT_ID('dbo.dim_location', 'U') IS NOT NULL
    DROP TABLE dbo.dim_location;
GO

CREATE TABLE dbo.dim_location (
    location_sk INT IDENTITY(1,1) PRIMARY KEY,           -- Surrogate key
    longitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    lsoav_of_accident_location VARCHAR(20),
    local_authority_district VARCHAR(50) NOT NULL,
    road_type VARCHAR(50) NOT NULL,
    speed_limit INT NOT NULL,
    junction_detail VARCHAR(50) NOT NULL,
    junction_control VARCHAR(50) NOT NULL,
    carriageway_hazards VARCHAR(100) NOT NULL,
    urban_or_rural_area VARCHAR(20) NOT NULL,
    trunk_road_flag BIT NOT NULL,
    first_road_class VARCHAR(10) NOT NULL,
    first_road_number INT NOT NULL,
    second_road_class VARCHAR(10) NOT NULL,
    second_road_number INT NOT NULL
);
GO


IF OBJECT_ID('dbo.dim_weather', 'U') IS NOT NULL
    DROP TABLE dbo.dim_weather;
GO

CREATE TABLE dbo.dim_weather (
    weather_sk INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate Key
    weather_date DATE NOT NULL,
    longitude FLOAT NOT NULL,                 -- zaokr¹glone do 1 miejsca
    latitude FLOAT NOT NULL,
    tavg FLOAT NOT NULL,
    tmin FLOAT NOT NULL,
    tmax FLOAT NOT NULL,
    prcp FLOAT NOT NULL,
    snow FLOAT NOT NULL,
    wdir FLOAT NOT NULL,
    wspd FLOAT NOT NULL,
    wpgt FLOAT NOT NULL,
    pres FLOAT NOT NULL,
    tsun FLOAT NOT NULL
);


IF OBJECT_ID('dbo.fact_collision', 'U') IS NOT NULL
    DROP TABLE dbo.fact_collision;
GO

CREATE TABLE dbo.fact_collision (
    accident_index VARCHAR(50) NOT NULL PRIMARY KEY,
    time_sk INT NOT NULL,
    location_sk INT NOT NULL,
    weather_sk INT NOT NULL,

    accident_severity SMALLINT NOT NULL,
    number_of_vehicles SMALLINT NOT NULL,
    number_of_casualties SMALLINT NOT NULL,
    police_attended BIT NOT NULL,
    escooter_involved BIT NOT NULL,
    enhanced_severity_collision SMALLINT NOT NULL,

    vehicle_type_car INT NOT NULL,
    vehicle_type_bus INT NOT NULL,
    vehicle_type_motorcycle INT NOT NULL,
    vehicle_type_goods INT NOT NULL,
    vehicle_type_other INT NOT NULL,
    vehicle_manoeuvre_turning_left INT NOT NULL,
    vehicle_manoeuvre_turning_right INT NOT NULL,
    vehicle_manoeuvre_overtaking INT NOT NULL,
    vehicle_left_hand_drive_count INT NOT NULL,
    avg_vehicle_age DECIMAL(10,2) NOT NULL,
    avg_engine_capacity_cc DECIMAL(10,2) NOT NULL,

    driver_sex_male INT NOT NULL,
    driver_sex_female INT NOT NULL,
    driver_age_band_0_25 INT NOT NULL,
    driver_age_band_26_50 INT NOT NULL,
    driver_age_band_51_plus INT NOT NULL,
    driver_purpose_commute INT NOT NULL,
    driver_purpose_education INT NOT NULL,
    driver_purpose_other INT NOT NULL,

    casualty_class_driver INT NOT NULL,
    casualty_class_passenger INT NOT NULL,
    casualty_class_pedestrian INT NOT NULL,
    casualty_severity_fatal INT NOT NULL,
    casualty_severity_serious INT NOT NULL,
    casualty_severity_slight INT NOT NULL,
    casualty_age_band_0_15 INT NOT NULL,
    casualty_age_band_16_30 INT NOT NULL,
    casualty_age_band_31_60 INT NOT NULL,
    casualty_age_band_60_plus INT NOT NULL,

    CONSTRAINT FK_fact_time FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk),
    CONSTRAINT FK_fact_location FOREIGN KEY (location_sk) REFERENCES dim_location(location_sk),
    CONSTRAINT FK_fact_weather FOREIGN KEY (weather_sk) REFERENCES dim_weather(weather_sk)
);
