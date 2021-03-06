set hiveconf:FLIGHT_DATA='/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data';
set hiveconf:AIRPORTS_DATA='/user/hadoop/ITBA/TP1/INPUT/SAMPLE/ref/airports.csv';

DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS tmp_table;
DROP TABLE IF EXISTS airports;
DROP TABLE IF EXISTS metric9;

create external table flights (
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
stored as textfile
location ${hiveconf:FLIGHT_DATA};


create external table airports (
  IATA string,
  name string,
  city string,
  state string,
  country string,
  latitude double,
  longintude double
)
row format delimited fields terminated by ','
stored as textfile
location ${hiveconf:AIRPORTS_DATA};

add jar Rank.jar;
create temporary function rank as 'udf.RankYear';

create table tmp_table (year int, origin string, dest string, total int);

insert overwrite table tmp_table
                    SELECT year, originIATA as origin, destIATA as dest, COUNT(*) AS total
                    FROM flights
                    GROUP BY year, originIATA, destIATA;



create external table metric9 (year int, origin string, dest string, total int, rank int)  row format delimited  fields terminated by ' '
 lines terminated by '\n'
 stored as textfile location '/user/hadoop/output/metric9';


insert overwrite table metric9 SELECT *
FROM
(
   SELECT *, rank(year) as row_number
   FROM (
        SELECT year, origin, dest, total
        FROM tmp_table
        DISTRIBUTE BY year
        SORT BY year, total desc
   ) A
) B
WHERE row_number <= 10
SORT BY year, row_number ;
