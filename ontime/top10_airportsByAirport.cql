
/* Queries for question 2.2:
CMI (University of Illinois Willard Airport)
BWI (Baltimore-Washington International Airport)
MIA (Miami International Airport)
LAX (Los Angeles International Airport)
IAH (George Bush Intercontinental Airport)
SFO (San Francisco International Airport)
*/

USE capstone;

SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'CMI';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'BWI';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'MIA';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'LAX';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'IAH';
SELECT origin, destination, depdelay FROM airportsbyairport WHERE origin = 'SFO';
