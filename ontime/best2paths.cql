
/* Queries for question 3.2:
CMI → ORD → LAX, 04/03/2008
JAX → DFW → CRP, 09/09/2008
SLC → BFL → LAX, 01/04/2008
LAX → SFO → PHX, 12/07/2008
DFW → ORD → DFW, 10/06/2008
LAX → ORD → JFK, 01/01/2008

http://stackoverflow.com/questions/19756368/bad-request-no-indexed-columns-present-in-by-columns-clause-with-equal-operator

CREATE INDEX best2path ON best2path (arrival2);

*/

USE capstone;

/* CMI → ORD → LAX, 04/03/2008 */

 SELECT flightnum1,
        origin1,
        dest1,
        departure1,
        arrival1,
        arrdelay1,
        flightnum2,
        origin2,
        dest2,
        departure2,
        arrival2,
        arrdelay2
   FROM best2path
  WHERE origin1 = 'CMI' AND
        dest1 = 'ORD' AND
        dest2 = 'LAX' AND
        arrival2 = '2008-03-04'
  ALLOW FILTERING;

/* JAX → DFW → CRP, 09/09/2008 */

SELECT flightnum1,
       origin1,
       dest1,
       departure1,
       arrival1,
       arrdelay1,
       flightnum2,
       origin2,
       dest2,
       departure2,
       arrival2,
       arrdelay2
  FROM best2path
 WHERE origin1 = 'JAX' AND
       dest1 = 'DFW' AND
       dest2 = 'CRP' AND
       arrival2 = '2008-09-09'
 ALLOW FILTERING;
