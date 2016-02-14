
/* Queries for question 2.3:
LGA → BOS
BOS → LGA
OKC → DFW
MSP → ATL
*/

USE capstone;

SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'LGA' AND destination = 'BOS';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'BOS' AND destination = 'LGA';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'OKC' AND destination = 'DFW';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'MSP' AND destination = 'ATL';
