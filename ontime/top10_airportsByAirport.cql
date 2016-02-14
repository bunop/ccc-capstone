
/* Queries for question 2.2:
* SRQ
* CMH
* JFK
* SEA
* BOS
*/

USE capstone;

SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'SRQ';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'CMH';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'JFK';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'SEA';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'BOS';
