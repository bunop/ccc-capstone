
/* Queries for question 3.2:
BOS → ATL → LAX, 03/04/2008
PHX → JFK → MSP, 07/09/2008
DFW → STL → ORD, 24/01/2008
LAX → MIA → LAX, 16/05/2008

http://stackoverflow.com/questions/19756368/bad-request-no-indexed-columns-present-in-by-columns-clause-with-equal-operator

CREATE INDEX best2path ON best2path (arrival2);

*/

USE capstone;

/* BOS → ATL → LAX, 03/04/2008 */

 SELECT startdate,
        flightnum1,
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
  WHERE origin1 = 'BOS' AND
        dest1 = 'ATL' AND
        dest2 = 'LAX' AND
        startdate = '03/04/2008';

/* PHX → JFK → MSP, 07/09/2008 */

 SELECT startdate,
        flightnum1,
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
  WHERE origin1 = 'PHX' AND
        dest1 = 'JFK' AND
        dest2 = 'MSP' AND
        startdate = '07/09/2008';

/* DFW → STL → ORD, 24/01/2008 */

 SELECT startdate,
        flightnum1,
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
  WHERE origin1 = 'DFW' AND
        dest1 = 'STL' AND
        dest2 = 'ORD' AND
        startdate = '24/01/2008';

/* LAX → MIA → LAX, 16/05/2008 */

 SELECT startdate,
        flightnum1,
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
  WHERE origin1 = 'LAX' AND
        dest1 = 'MIA' AND
        dest2 = 'LAX' AND
        startdate = '16/05/2008';
