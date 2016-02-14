
/* Queries for question 2.1
* SRQ
* CMH
* JFK
* SEA
* BOS
*/

USE capstone;

/*
DROP TABLE carriersbyairport;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
COPY carriersbyairport (origin, airlineid, airline, depdelay) FROM 'top10_carriersByAirport/part-r-00000' WITH DELIMITER=',';
*/

SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SRQ';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'CMH';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'JFK';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SEA';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'BOS';
