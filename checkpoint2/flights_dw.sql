SHOW DATABASES;

DROP DATABASE flights_dw;
CREATE DATABASE flights_dw;



SELECT * FROM flights_dw;

USE flights_dw;
SHOW TABLES;


SELECT * FROM flight LIMIT 10;
SELECT * FROM route LIMIT 10;
SELECT * FROM airline LIMIT 10;
SELECT * FROM aircraft LIMIT 10;
SELECT * FROM dep_delay LIMIT 10;
SELECT * FROM arr_delay LIMIT 10;




-- star shema
SELECT * FROM fact_flight LIMIT 10;
