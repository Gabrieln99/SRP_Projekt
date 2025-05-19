SHOW DATABASES;

DROP DATABASE flights_db;
CREATE DATABASE flights_db;



SELECT * FROM flights_db;

USE flights_db;
SHOW TABLES;

DELETE FROM airline;

SELECT * FROM fact_flight LIMIT 10;

SELECT * FROM flight LIMIT 10;
SELECT * FROM route LIMIT 10;
SELECT * FROM airline LIMIT 10;
SELECT * FROM aircraft LIMIT 10;
SELECT * FROM dep_delay LIMIT 10;
SELECT * FROM arr_delay LIMIT 10;

SELECT DISTINCT airline_name FROM airline;
SELECT COUNT(DISTINCT airline_name) AS broj_aviokompanija FROM airline;



SELECT 'aircraft' AS table_name, COUNT(*) AS row_count FROM aircraft
UNION
SELECT 'airline', COUNT(*) FROM airline
UNION
SELECT 'arr_delay', COUNT(*) FROM arr_delay
UNION
SELECT 'dep_delay', COUNT(*) FROM dep_delay
UNION
SELECT 'flight', COUNT(*) FROM flight;

SELECT * FROM aircraft LIMIT 10;
SELECT * FROM airline LIMIT 10;
SELECT * FROM arr_delay LIMIT 10;
SELECT * FROM dep_delay LIMIT 10;
SELECT * FROM flight LIMIT 10;




# za star shemu

DESCRIBE fact_flight;

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'flights_db'
AND TABLE_NAME = 'flight';

SELECT air_time
FROM flights_db.flight;

SELECT distance
FROM flights_db.route;

SELECT flight_num
FROM flights_db.flight;



# radi

 SELECT f.year, f.month, f.day, f.day_of_week, f.dep_time, f.arr_time, 
               f.hour, f.minute, d.reason_dep_delay, d.dep_delay_time, 
               a.reason_arr_delay, a.arr_delay_time, 
               r.origin, r.destination, r.sched_dep_time, r.sched_arr_time, 
               ac.tailnum, al.carrier, al.airline_name
        FROM flight f
        LEFT JOIN dep_delay d ON f.dep_delay_fk = d.id
        LEFT JOIN arr_delay a ON f.arr_delay_fk = a.id
        LEFT JOIN route r ON f.route_fk = r.id
        LEFT JOIN aircraft ac ON f.aircraft_fk = ac.id
        LEFT JOIN airline al ON ac.airline_fk = al.id
        ORDER BY f.id ASC;





# Vremenska dimenzija - hijerarhiska 
SELECT month, COUNT(*) FROM flight WHERE year = 2013 GROUP BY month;


#nemam grad trebat ce ga dodati po tipu origin i dest
SELECT airport.name, COUNT(*) 
FROM flight 
JOIN airport ON flight.origin_fk = airport.id 
WHERE airport.city = 'New York' 
GROUP BY airport.name;


# hjerarhiska dimenzija- Razlozi kašnjenja hijerarhija (Delay Hierarchy)


#letna hijerarhija



# sezonska hijerarhija 
SELECT 
    IF(month IN (12, 1, 2), 'Winter',
       IF(month IN (3, 4, 5), 'Spring',
          IF(month IN (6, 7, 8), 'Summer', 'Autumn')
       )
    ) AS season,
    AVG(dep_delay) AS avg_delay
FROM flight 
GROUP BY season;



#tip aviona hijerarhija 

SELECT aircraft.model, COUNT(*)
FROM flight 
JOIN aircraft ON flight.aircraft_fk = aircraft.id
GROUP BY aircraft.model
ORDER BY COUNT(*) DESC
LIMIT 1;


#raspored polijetanja hijerarhija 
# dodati  Dimenzija rute ->obavezno

SELECT 
    CASE 
        WHEN hour BETWEEN 5 AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_of_day,
    AVG(dep_delay) AS avg_delay
FROM flight 
GROUP BY time_of_day;


