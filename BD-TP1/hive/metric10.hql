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

create external table metric10 (my_date string, flight_count string, added int)  row format delimited  fields terminated by ' '
 lines terminated by '\n'
 stored as textfile location '/user/hadoop/output';


insert overwrite table metric10 SELECT tmp_table.my_date, COUNT(*), SUM(cancelled)
FROM
        (SELECT CONCAT(year, '-', month, '-', dayOfMonth) as my_date, cancelled
         FROM flights
         WHERE year = 2001 and month = 9) tmp_table
GROUP BY tmp_table.my_date;
