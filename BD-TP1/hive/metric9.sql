DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS airports;
create table flights (
  year int,
  month int,
  dayOfMonth int,
  dayOfWeek int,
  depTime string,
  crsDepTime string,
  arrTime string,
  crsArrTime string,
  carrierCode string,
  flightNum int,
  tailNum string,
  actualElapsedTime int,
  crsElapsedTime int,
  airTime int,
  arrDelay int,
  depDelay int,
  originIATA string,
  destIATA string,
  distance int,
  taxiIn int,
  taxiOut int,
  cancelled int,
  cancellationCode string,
  diverted int,
    carrierDelay int,
  weatherDelay int,
    nasDelay int,
  securityDelay int,
  LateAircraftDelay int
)
row format delimited fields terminated by ','
stored as textfile;

LOAD DATA INPATH '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data' into table flights;

create table airports (
  IATA string,
  name string,
  city string,
  state string,
  country string,
  latitude double,
  longintude double
)
row format delimited fields terminated by ','
stored as textfile;

LOAD DATA INPATH '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/ref/airports.csv' into table airports;

add jar Rank.jar;
create temporary function rank as 'udf.RankYear';


SELECT * 
FROM 
    ( SELECT *, rank(tmp_table.year) as rank
      FROM
          ( SELECT *
            FROM
                (SELECT a1.name as origin, a2.name as dest, year, COUNT(year) AS total
                 FROM flights
                 JOIN airports a1 ON regexp_replace(a1.IATA, '\"', '') = flights.originIATA
                 JOIN airports a2 ON regexp_replace(a2.IATA, '\"', '') = flights.destIATA
                 GROUP BY a1.name, a2.name, year
                 ) C
            DISTRIBUTE by C.year
            SORT BY total
          ) tmp_table
    ) A
WHERE rank < 10
SORT BY year, rank;

