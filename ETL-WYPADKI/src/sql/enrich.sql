-- 1. Dane z wypadków
WITH base_c AS (
    SELECT 
        accident_index,
        CONVERT(DATE, date, 103) AS [date],
		TRY_CAST([time] AS TIME) AS accident_time,
        TRY_CAST(longitude AS FLOAT) AS longitude,
        TRY_CAST(latitude AS FLOAT) AS latitude,
        CASE WHEN ISNUMERIC(accident_severity) = 1 AND accident_severity NOT IN ('-1', 'NULL') 
             THEN CAST(accident_severity AS SMALLINT) ELSE NULL END AS accident_severity,
        CASE WHEN ISNUMERIC(number_of_vehicles) = 1 AND number_of_vehicles NOT IN ('-1', 'NULL') 
             THEN CAST(number_of_vehicles AS SMALLINT) ELSE NULL END AS number_of_vehicles,
        CASE WHEN ISNUMERIC(number_of_casualties) = 1 AND number_of_casualties NOT IN ('-1', 'NULL') 
             THEN CAST(number_of_casualties AS SMALLINT) ELSE NULL END AS number_of_casualties,
        CASE WHEN did_police_officer_attend_scene_of_accident = '1' THEN 1 ELSE 0 END AS police_attended,

		CAST(TRY_CAST(enhanced_severity_collision AS INT) AS SMALLINT) AS enhanced_severity_collision,

        CASE WHEN ISNUMERIC(location_easting_osgr) = 1 AND location_easting_osgr NOT IN ('-1', 'NULL') 
             THEN CAST(location_easting_osgr AS INT) ELSE NULL END AS location_easting_osgr,
        CASE WHEN ISNUMERIC(location_northing_osgr) = 1 AND location_northing_osgr NOT IN ('-1', 'NULL') 
             THEN CAST(location_northing_osgr AS INT) ELSE NULL END AS location_northing_osgr,

        local_authority_district,
        local_authority_ons_district,
        local_authority_highway,

        CASE WHEN ISNUMERIC(first_road_class) = 1 AND first_road_class NOT IN ('-1', 'NULL') 
             THEN CAST(first_road_class AS INT) ELSE NULL END AS first_road_class,
        CASE WHEN ISNUMERIC(first_road_number) = 1 AND first_road_number NOT IN ('-1', 'NULL') 
             THEN CAST(first_road_number AS INT) ELSE NULL END AS first_road_number,
        CASE WHEN ISNUMERIC(second_road_class) = 1 AND second_road_class NOT IN ('-1', 'NULL') 
             THEN CAST(second_road_class AS INT) ELSE NULL END AS second_road_class,
      --  CASE WHEN ISNUMERIC(second_road_number) = 1 AND second_road_number NOT IN ('-1', 'NULL') 
        --     THEN CAST(second_road_number AS INT) ELSE NULL END AS second_road_number,
		ISNULL(
    CASE WHEN ISNUMERIC(second_road_number) = 1 
         THEN CAST(second_road_number AS INT) 
         ELSE NULL END, 
    -1
) AS second_road_number,

        road_type,

        CASE WHEN ISNUMERIC(speed_limit) = 1 AND speed_limit NOT IN ('-1', 'NULL') 
             THEN CAST(speed_limit AS INT) ELSE NULL END AS speed_limit,
        CASE WHEN ISNUMERIC(junction_detail) = 1 AND junction_detail NOT IN ('-1', 'NULL') 
             THEN CAST(junction_detail AS INT) ELSE NULL END AS junction_detail,
        --CASE WHEN ISNUMERIC(junction_control) = 1 AND junction_control NOT IN ('-1', 'NULL') 
         --    THEN CAST(junction_control AS INT) ELSE NULL END AS junction_control,
		 ISNULL(
    CASE WHEN ISNUMERIC(junction_control) = 1 
         THEN CAST(junction_control AS INT) 
         ELSE NULL END, 
    -1
) AS junction_control,
        CASE WHEN ISNUMERIC(pedestrian_crossing_human_control) = 1 AND pedestrian_crossing_human_control NOT IN ('-1', 'NULL') 
             THEN CAST(pedestrian_crossing_human_control AS INT) ELSE NULL END AS pedestrian_crossing_human_control,
        CASE WHEN ISNUMERIC(pedestrian_crossing_physical_facilities) = 1 AND pedestrian_crossing_physical_facilities NOT IN ('-1', 'NULL') 
             THEN CAST(pedestrian_crossing_physical_facilities AS INT) ELSE NULL END AS pedestrian_crossing_physical_facilities,
        CASE WHEN ISNUMERIC(light_conditions) = 1 AND light_conditions NOT IN ('-1', 'NULL') 
             THEN CAST(light_conditions AS INT) ELSE NULL END AS light_conditions,
        CASE WHEN ISNUMERIC(weather_conditions) = 1 AND weather_conditions NOT IN ('-1', 'NULL') 
             THEN CAST(weather_conditions AS INT) ELSE NULL END AS weather_conditions,
        CASE WHEN ISNUMERIC(road_surface_conditions) = 1 AND road_surface_conditions NOT IN ('-1', 'NULL') 
             THEN CAST(road_surface_conditions AS INT) ELSE NULL END AS road_surface_conditions,
        CASE WHEN ISNUMERIC(special_conditions_at_site) = 1 AND special_conditions_at_site NOT IN ('-1', 'NULL') 
             THEN CAST(special_conditions_at_site AS INT) ELSE NULL END AS special_conditions_at_site,
        CASE WHEN ISNUMERIC(carriageway_hazards) = 1 AND carriageway_hazards NOT IN ('-1', 'NULL') 
             THEN CAST(carriageway_hazards AS INT) ELSE NULL END AS carriageway_hazards,
        CASE WHEN ISNUMERIC(urban_or_rural_area) = 1 AND urban_or_rural_area NOT IN ('-1', 'NULL') 
             THEN CAST(urban_or_rural_area AS INT) ELSE NULL END AS urban_or_rural_area,

        CASE WHEN trunk_road_flag = '1' THEN 1 ELSE 0 END AS trunk_road_flag,
        lsoa_of_accident_location
    FROM dbo.Stg_Collisions_Raw
)

,


-- 2. Agregacja danych o pojazdach
agg_v AS (
    SELECT
        accident_index,
        MAX(CASE WHEN escooter_flag = '1' THEN 1 ELSE 0 END) AS escooter_involved,
        SUM(CASE WHEN vehicle_type = '9' THEN 1 ELSE 0 END) AS vehicle_type_car,
        SUM(CASE WHEN vehicle_type = '1' THEN 1 ELSE 0 END) AS vehicle_type_bus,
        SUM(CASE WHEN vehicle_type = '2' THEN 1 ELSE 0 END) AS vehicle_type_motorcycle,
        SUM(CASE WHEN vehicle_type = '3' THEN 1 ELSE 0 END) AS vehicle_type_goods,
        SUM(CASE WHEN vehicle_type NOT IN ('1','2','3','9') THEN 1 ELSE 0 END) AS vehicle_type_other,
        
        SUM(CASE WHEN vehicle_manoeuvre = '18' THEN 1 ELSE 0 END) AS vehicle_manoeuvre_turning_left,
        SUM(CASE WHEN vehicle_manoeuvre = '19' THEN 1 ELSE 0 END) AS vehicle_manoeuvre_turning_right,
        SUM(CASE WHEN vehicle_manoeuvre = '14' THEN 1 ELSE 0 END) AS vehicle_manoeuvre_overtaking,
        SUM(CASE WHEN vehicle_left_hand_drive = '1' THEN 1 ELSE 0 END) AS vehicle_left_hand_drive_count,
        
        CAST(AVG(CASE 
            WHEN TRY_CAST(age_of_vehicle AS FLOAT) BETWEEN 0 AND 100 
            THEN TRY_CAST(age_of_vehicle AS FLOAT) 
            ELSE NULL END) AS DECIMAL(10,2)) AS avg_vehicle_age,
        
        CAST(AVG(CASE 
            WHEN TRY_CAST(engine_capacity_cc AS FLOAT) BETWEEN 100 AND 20000 
            THEN TRY_CAST(engine_capacity_cc AS FLOAT) 
            ELSE NULL END) AS DECIMAL(10,2)) AS avg_engine_capacity_cc,

        SUM(CASE WHEN sex_of_driver = '1' THEN 1 ELSE 0 END) AS driver_sex_male,
        SUM(CASE WHEN sex_of_driver = '2' THEN 1 ELSE 0 END) AS driver_sex_female,
        
        SUM(CASE WHEN TRY_CAST(age_of_driver AS INT) BETWEEN 0 AND 25 THEN 1 ELSE 0 END) AS driver_age_band_0_25,
        SUM(CASE WHEN TRY_CAST(age_of_driver AS INT) BETWEEN 26 AND 50 THEN 1 ELSE 0 END) AS driver_age_band_26_50,
        SUM(CASE WHEN TRY_CAST(age_of_driver AS INT) > 50 THEN 1 ELSE 0 END) AS driver_age_band_51_plus,

        SUM(CASE WHEN journey_purpose_of_driver = '1' THEN 1 ELSE 0 END) AS driver_purpose_commute,
        SUM(CASE WHEN journey_purpose_of_driver = '2' THEN 1 ELSE 0 END) AS driver_purpose_education,
        SUM(CASE WHEN journey_purpose_of_driver NOT IN ('1','2') THEN 1 ELSE 0 END) AS driver_purpose_other

    FROM dbo.Stg_Vehicles_Raw
    GROUP BY accident_index
),

-- 3. Agregacja danych o ofiarach
agg_ca AS (
    SELECT 
        accident_index,
        SUM(CASE WHEN casualty_class = '1' THEN 1 ELSE 0 END) AS casualty_class_driver,
        SUM(CASE WHEN casualty_class = '2' THEN 1 ELSE 0 END) AS casualty_class_passenger,
        SUM(CASE WHEN casualty_class = '3' THEN 1 ELSE 0 END) AS casualty_class_pedestrian,

        SUM(CASE WHEN casualty_severity = '1' THEN 1 ELSE 0 END) AS casualty_severity_fatal,
        SUM(CASE WHEN casualty_severity = '2' THEN 1 ELSE 0 END) AS casualty_severity_serious,
        SUM(CASE WHEN casualty_severity = '3' THEN 1 ELSE 0 END) AS casualty_severity_slight,

        SUM(CASE WHEN TRY_CAST(age_of_casualty AS INT) BETWEEN 0 AND 15 THEN 1 ELSE 0 END) AS casualty_age_band_0_15,
        SUM(CASE WHEN TRY_CAST(age_of_casualty AS INT) BETWEEN 16 AND 30 THEN 1 ELSE 0 END) AS casualty_age_band_16_30,
        SUM(CASE WHEN TRY_CAST(age_of_casualty AS INT) BETWEEN 31 AND 60 THEN 1 ELSE 0 END) AS casualty_age_band_31_60,
        SUM(CASE WHEN TRY_CAST(age_of_casualty AS INT) > 60 THEN 1 ELSE 0 END) AS casualty_age_band_60_plus

    FROM dbo.Stg_Casualties_Raw
    GROUP BY accident_index
)

INSERT INTO dbo.Stg_Collisions_Enriched (
    accident_index, [date],accident_time, longitude, latitude, accident_severity, number_of_vehicles,
    number_of_casualties, police_attended, escooter_involved, enhanced_severity_collision,

    location_easting_osgr, location_northing_osgr,
    local_authority_district, local_authority_ons_district, local_authority_highway,
    first_road_class, first_road_number, second_road_class, second_road_number,
    road_type, speed_limit, junction_detail, junction_control,
    pedestrian_crossing_human_control, pedestrian_crossing_physical_facilities,
    light_conditions, weather_conditions, road_surface_conditions,
    special_conditions_at_site, carriageway_hazards, urban_or_rural_area,
    trunk_road_flag, lsoa_of_accident_location,

    -- Dotychczasowe dane
    vehicle_type_car, vehicle_type_bus, vehicle_type_motorcycle, vehicle_type_goods, vehicle_type_other,
    vehicle_manoeuvre_turning_left, vehicle_manoeuvre_turning_right, vehicle_manoeuvre_overtaking,
    vehicle_left_hand_drive_count, avg_vehicle_age, avg_engine_capacity_cc,
    driver_sex_male, driver_sex_female,
    driver_age_band_0_25, driver_age_band_26_50, driver_age_band_51_plus,
    driver_purpose_commute, driver_purpose_education, driver_purpose_other,
    casualty_class_driver, casualty_class_passenger, casualty_class_pedestrian,
    casualty_severity_fatal, casualty_severity_serious, casualty_severity_slight,
    casualty_age_band_0_15, casualty_age_band_16_30, casualty_age_band_31_60, casualty_age_band_60_plus
)
SELECT 
    c.accident_index, c.[date],c.accident_time, c.longitude, c.latitude, c.accident_severity,
    c.number_of_vehicles, c.number_of_casualties, c.police_attended,
    ISNULL(v.escooter_involved, 0),
    c.enhanced_severity_collision,

    -- Nowe kolumny lokalizacji i drogi
    c.location_easting_osgr, c.location_northing_osgr,
    c.local_authority_district, c.local_authority_ons_district, c.local_authority_highway,
    c.first_road_class, c.first_road_number, c.second_road_class, c.second_road_number,
    c.road_type, c.speed_limit, c.junction_detail, c.junction_control,
    c.pedestrian_crossing_human_control, c.pedestrian_crossing_physical_facilities,
    c.light_conditions, c.weather_conditions, c.road_surface_conditions,
    c.special_conditions_at_site, c.carriageway_hazards, c.urban_or_rural_area,
    c.trunk_road_flag, c.lsoa_of_accident_location,

    -- Dane z pojazdów i ofiar
    ISNULL(v.vehicle_type_car, 0), ISNULL(v.vehicle_type_bus, 0),
    ISNULL(v.vehicle_type_motorcycle, 0), ISNULL(v.vehicle_type_goods, 0),
    ISNULL(v.vehicle_type_other, 0),
    ISNULL(v.vehicle_manoeuvre_turning_left, 0),
    ISNULL(v.vehicle_manoeuvre_turning_right, 0),
    ISNULL(v.vehicle_manoeuvre_overtaking, 0),
    ISNULL(v.vehicle_left_hand_drive_count, 0),
    ISNULL(v.avg_vehicle_age, 0),
    ISNULL(v.avg_engine_capacity_cc, 0),
    ISNULL(v.driver_sex_male, 0), ISNULL(v.driver_sex_female, 0),
    ISNULL(v.driver_age_band_0_25, 0), ISNULL(v.driver_age_band_26_50, 0),
    ISNULL(v.driver_age_band_51_plus, 0),
    ISNULL(v.driver_purpose_commute, 0), ISNULL(v.driver_purpose_education, 0),
    ISNULL(v.driver_purpose_other, 0),
    ISNULL(ca.casualty_class_driver, 0), ISNULL(ca.casualty_class_passenger, 0),
    ISNULL(ca.casualty_class_pedestrian, 0),
    ISNULL(ca.casualty_severity_fatal, 0), ISNULL(ca.casualty_severity_serious, 0),
    ISNULL(ca.casualty_severity_slight, 0),
    ISNULL(ca.casualty_age_band_0_15, 0), ISNULL(ca.casualty_age_band_16_30, 0),
    ISNULL(ca.casualty_age_band_31_60, 0), ISNULL(ca.casualty_age_band_60_plus, 0)

FROM base_c c
LEFT JOIN agg_v v ON c.accident_index = v.accident_index
LEFT JOIN agg_ca ca ON c.accident_index = ca.accident_index;
