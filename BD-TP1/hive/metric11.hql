set hiveconf:FLIGHT_DATA='/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data';

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

LOAD DATA INPATH ${hiveconf:FLIGHT_DATA} into table flights;

create external table metric11 (origin string, last_dep int)  row format delimited  fields terminated by ' '
 lines terminated by '\n'
 stored as textfile location '/user/hadoop/output/metric11';

insert overwrite table metric11 SELECT originIATA, MAX(cast(depTime as int))
FROM flights
WHERE year = 2001 and month = 9 and dayOfMonth = 11
GROUP BY originIATA;



