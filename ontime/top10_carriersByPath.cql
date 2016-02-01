
/* Queries for question 2.3:
CMI → ORD
IND → CMH
DFW → IAH
LAX → SFO
JFK → LAX
ATL → PHX
*/

USE capstone;

SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'CMI' AND destination = 'ORD';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'IND' AND destination = 'CMH';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'DFW' AND destination = 'IAH';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'LAX' AND destination = 'SFO';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'JFK' AND destination = 'LAX';
SELECT origin, destination, airlineid, arrdelay, airline FROM carriersbypath WHERE origin = 'ATL' AND destination = 'PHX';
