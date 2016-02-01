
/* Queries for question 2.1
CMI (University of Illinois Willard Airport)
BWI (Baltimore-Washington International Airport)
MIA (Miami International Airport)
LAX (Los Angeles International Airport)
IAH (George Bush Intercontinental Airport)
SFO (San Francisco International Airport)
*/

USE capstone;

/*
DROP TABLE carriersbyairport;
CREATE TABLE carriersbyairport ( origin TEXT, airlineid INT, airline TEXT, depdelay FLOAT, PRIMARY KEY(origin, depdelay));
COPY carriersbyairport (origin, airlineid, airline, depdelay) FROM 'top10_carriersByAirport/part-r-00000' WITH DELIMITER=',';
*/

SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'CMI';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'BWI';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'MIA';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'LAX';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'IAH';
SELECT origin, airlineid, depdelay, airline FROM carriersbyairport WHERE origin = 'SFO';
