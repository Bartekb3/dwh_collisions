-- Tworzenie bazy, jeœli nie istnieje
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DWH_Staging')
    CREATE DATABASE DWH_Staging;
GO

USE DWH_Staging;
GO

-- COLLISIONS
IF OBJECT_ID('dbo.Stg_Collisions_Raw', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stg_Collisions_Raw (
        accident_index VARCHAR(50) PRIMARY KEY,
        accident_year VARCHAR(MAX) NULL,
        accident_reference VARCHAR(MAX) NULL,
        location_easting_osgr VARCHAR(MAX) NULL,
        location_northing_osgr VARCHAR(MAX) NULL,
        longitude VARCHAR(MAX) NULL,
        latitude VARCHAR(MAX) NULL,
        police_force VARCHAR(MAX) NULL,
        accident_severity VARCHAR(MAX) NULL,
        number_of_vehicles VARCHAR(MAX) NULL,
        number_of_casualties VARCHAR(MAX) NULL,
        [date] VARCHAR(MAX) NULL,
        day_of_week VARCHAR(MAX) NULL,
        [time] VARCHAR(MAX) NULL,
        local_authority_district VARCHAR(MAX) NULL,
        local_authority_ons_district VARCHAR(MAX) NULL,
        local_authority_highway VARCHAR(MAX) NULL,
        first_road_class VARCHAR(MAX) NULL,
        first_road_number VARCHAR(MAX) NULL,
        road_type VARCHAR(MAX) NULL,
        speed_limit VARCHAR(MAX) NULL,
        junction_detail VARCHAR(MAX) NULL,
        junction_control VARCHAR(MAX) NULL,
        second_road_class VARCHAR(MAX) NULL,
        second_road_number VARCHAR(MAX) NULL,
        pedestrian_crossing_human_control VARCHAR(MAX) NULL,
        pedestrian_crossing_physical_facilities VARCHAR(MAX) NULL,
        light_conditions VARCHAR(MAX) NULL,
        weather_conditions VARCHAR(MAX) NULL,
        road_surface_conditions VARCHAR(MAX) NULL,
        special_conditions_at_site VARCHAR(MAX) NULL,
        carriageway_hazards VARCHAR(MAX) NULL,
        urban_or_rural_area VARCHAR(MAX) NULL,
        did_police_officer_attend_scene_of_accident VARCHAR(MAX) NULL,
        trunk_road_flag VARCHAR(MAX) NULL,
        lsoa_of_accident_location VARCHAR(MAX) NULL,
        enhanced_severity_collision VARCHAR(MAX) NULL
    );
END
GO

-- VEHICLES
IF OBJECT_ID('dbo.Stg_Vehicles_Raw', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stg_Vehicles_Raw (
        accident_index VARCHAR(50) NOT NULL,
        vehicle_reference VARCHAR(50) NOT NULL,
        accident_year VARCHAR(MAX) NULL,
        accident_reference VARCHAR(MAX) NULL,
        vehicle_type VARCHAR(MAX) NULL,
        towing_and_articulation VARCHAR(MAX) NULL,
        vehicle_manoeuvre VARCHAR(MAX) NULL,
        vehicle_direction_from VARCHAR(MAX) NULL,
        vehicle_direction_to VARCHAR(MAX) NULL,
        vehicle_location_restricted_lane VARCHAR(MAX) NULL,
        junction_location VARCHAR(MAX) NULL,
        skidding_and_overturning VARCHAR(MAX) NULL,
        hit_object_in_carriageway VARCHAR(MAX) NULL,
        vehicle_leaving_carriageway VARCHAR(MAX) NULL,
        hit_object_off_carriageway VARCHAR(MAX) NULL,
        first_point_of_impact VARCHAR(MAX) NULL,
        vehicle_left_hand_drive VARCHAR(MAX) NULL,
        journey_purpose_of_driver VARCHAR(MAX) NULL,
        sex_of_driver VARCHAR(MAX) NULL,
        age_of_driver VARCHAR(MAX) NULL,
        age_band_of_driver VARCHAR(MAX) NULL,
        engine_capacity_cc VARCHAR(MAX) NULL,
        propulsion_code VARCHAR(MAX) NULL,
        age_of_vehicle VARCHAR(MAX) NULL,
        generic_make_model VARCHAR(MAX) NULL,
        driver_imd_decile VARCHAR(MAX) NULL,
        driver_home_area_type VARCHAR(MAX) NULL,
        lsoa_of_driver VARCHAR(MAX) NULL,
        escooter_flag VARCHAR(MAX) NULL,
        dir_from_e VARCHAR(MAX) NULL,
        dir_from_n VARCHAR(MAX) NULL,
        dir_to_e VARCHAR(MAX) NULL,
        dir_to_n VARCHAR(MAX) NULL,
        driver_distance_banding VARCHAR(MAX) NULL,
 --       CONSTRAINT PK_Stg_Vehicles PRIMARY KEY (accident_index, vehicle_reference),
        CONSTRAINT FK_Vehicle_Collision FOREIGN KEY (accident_index)
            REFERENCES dbo.Stg_Collisions_Raw(accident_index)
    );
END
GO

-- CASUALTIES
IF OBJECT_ID('dbo.Stg_Casualties_Raw', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stg_Casualties_Raw (
        accident_index VARCHAR(50) NOT NULL,
        casualty_reference VARCHAR(50) NOT NULL,
        accident_year VARCHAR(MAX) NULL,
        accident_reference VARCHAR(MAX) NULL,
        vehicle_reference VARCHAR(50) NOT NULL,
        casualty_class VARCHAR(MAX) NULL,
        sex_of_casualty VARCHAR(MAX) NULL,
        age_of_casualty VARCHAR(MAX) NULL,
        age_band_of_casualty VARCHAR(MAX) NULL,
        casualty_severity VARCHAR(MAX) NULL,
        pedestrian_location VARCHAR(MAX) NULL,
        pedestrian_movement VARCHAR(MAX) NULL,
        car_passenger VARCHAR(MAX) NULL,
        bus_or_coach_passenger VARCHAR(MAX) NULL,
        pedestrian_road_maintenance_worker VARCHAR(MAX) NULL,
        casualty_type VARCHAR(MAX) NULL,
        casualty_home_area_type VARCHAR(MAX) NULL,
        casualty_imd_decile VARCHAR(MAX) NULL,
        lsoa_of_casualty VARCHAR(MAX) NULL,
        enhanced_casualty_severity VARCHAR(MAX) NULL,
        casualty_distance_banding VARCHAR(MAX) NULL,
    --    CONSTRAINT PK_Stg_Casualties PRIMARY KEY (accident_index, casualty_reference, vehicle_reference),
        CONSTRAINT FK_Casualty_Collision FOREIGN KEY (accident_index)
            REFERENCES dbo.Stg_Collisions_Raw(accident_index)
    );
END
GO

IF OBJECT_ID('dbo.Stg_Collisions_Enriched', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stg_Collisions_Enriched (
        accident_index VARCHAR(50) PRIMARY KEY,
        [date] DATE,
        accident_time TIME,
        longitude FLOAT,
        latitude FLOAT,
        accident_severity SMALLINT,
        number_of_vehicles SMALLINT,
        number_of_casualties SMALLINT,
        police_attended BIT,
        escooter_involved BIT,
        enhanced_severity_collision SMALLINT,

        -- Nowe kolumny lokalizacyjne i drogowe
        location_easting_osgr INT,
        location_northing_osgr INT,
        local_authority_district VARCHAR(100),
        local_authority_ons_district VARCHAR(100),
        local_authority_highway VARCHAR(100),
        first_road_class INT,
        first_road_number INT,
        second_road_class INT,
        second_road_number INT,
        road_type VARCHAR(50),
        speed_limit INT,
        junction_detail INT,
        junction_control INT,
        pedestrian_crossing_human_control INT,
        pedestrian_crossing_physical_facilities INT,
        light_conditions INT,
        weather_conditions INT,
        road_surface_conditions INT,
        special_conditions_at_site INT,
        carriageway_hazards INT,
        urban_or_rural_area INT,
        trunk_road_flag BIT,
        lsoa_of_accident_location VARCHAR(50),

        -- Dotychczasowe kolumny pojazdów i ofiar
        vehicle_type_car INT,
        vehicle_type_bus INT,
        vehicle_type_motorcycle INT,
        vehicle_type_goods INT,
        vehicle_type_other INT,

        vehicle_manoeuvre_turning_left INT,
        vehicle_manoeuvre_turning_right INT,
        vehicle_manoeuvre_overtaking INT,

        vehicle_left_hand_drive_count INT,
        avg_vehicle_age DECIMAL(10,2),
        avg_engine_capacity_cc DECIMAL(10,2),

        driver_sex_male INT,
        driver_sex_female INT,

        driver_age_band_0_25 INT,
        driver_age_band_26_50 INT,
        driver_age_band_51_plus INT,

        driver_purpose_commute INT,
        driver_purpose_education INT,
        driver_purpose_other INT,

        casualty_class_driver INT,
        casualty_class_passenger INT,
        casualty_class_pedestrian INT,

        casualty_severity_fatal INT,
        casualty_severity_serious INT,
        casualty_severity_slight INT,

        casualty_age_band_0_15 INT,
        casualty_age_band_16_30 INT,
        casualty_age_band_31_60 INT,
        casualty_age_band_60_plus INT
    );
END
GO

IF OBJECT_ID('dbo.Stg_Weather', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Stg_Weather (
        weather_date DATE,
        longitude FLOAT,
        latitude FLOAT,
        tavg FLOAT,
        tmin FLOAT,
        tmax FLOAT,
        prcp FLOAT,
        snow FLOAT,
        wdir FLOAT,
        wspd FLOAT,
        wpgt FLOAT,
        pres FLOAT,
        tsun FLOAT
    );
END;

