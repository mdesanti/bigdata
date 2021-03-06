%default PIGGYBANK_PATH '/home/hadoop/pig-0.11.1/contrib/piggybank/java/piggybank.jar'
%default FLIGHTS_PATH '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data/'
%default AIRPORTS_HBASE_PATH 'hbase://itba_tp1_airports'
%default PROTOBUF_PATH '/home/hadoop/hbase-0.94.6.1/lib/protobuf-java-2.4.0a.jar'
%default OUTPUT_PATH 'metric2/output'

REGISTER '$PIGGYBANK_PATH';
REGISTER '$PROTOBUF_PATH';

%default SELECTED_AIRPORT '';

flights = LOAD '$FLIGHTS_PATH' 
          USING org.apache.pig.piggybank.storage.CSVLoader()
          AS (Year:chararray, Month:chararray, DayofMonth:chararray, DayOfWeek:chararray,
              DepTime:chararray, CRSDepTime:chararray, ArrTime:chararray, CRSArrTime:chararray,
              UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,  ActualElapsedTime:chararray,
              CRSElapsedTime:chararray, AirTime:chararray, ArrDelay:chararray, DepDelay:long, origin:chararray,
              Dest:chararray, Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int,
              CancellationCode:chararray, Diverted:int, CarrierDelay:chararray, WeatherDelay:chararray,
              NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

airports = LOAD '$AIRPORTS_HBASE_PATH'
           USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:airport', '-loadKey true')
           AS (id:chararray, airport:chararray);
/* We want for each day the amount of delayed planes, the amount of cancelled, the amount of diverted and the amount of diverted by weather.
   When we generate simple_flights, we map the departure delay to a 1 or a 0 to make the addition easier (same with cancellation code)
*/

filtered = FILTER flights BY year == 2005;

simple_flights = FOREACH filtered
                 GENERATE   CONCAT((chararray)Year,
                            CONCAT('/',
                            CONCAT((chararray)Month,
                            CONCAT('/', (chararray)DayofMonth)))) AS day:chararray, /* Fecha */
                            (origin == '$SELECTED_AIRPORT' or '$SELECTED_AIRPORT' == '' ? (DepDelay > 0 ? 1:0) : 0) as delayed:long,
                            (origin == '$SELECTED_AIRPORT' or '$SELECTED_AIRPORT' == '' ? (DepDelay > 0 ? DepDelay : 0) : 0) as DepDelay,
                            (origin == '$SELECTED_AIRPORT' or '$SELECTED_AIRPORT' == '' ? Cancelled : 0) as Cancelled,
                            (origin == '$SELECTED_AIRPORT' or '$SELECTED_AIRPORT' == '' ? Diverted : 0) as Diverted,
                            (origin == '$SELECTED_AIRPORT' or '$SELECTED_AIRPORT' == '' ? (CancellationCode == 'B' ? 1:0) : 0) AS weather_cancellation:long;



grouped = GROUP simple_flights BY (day);

summed = FOREACH grouped
         GENERATE group,
                  SUM(simple_flights.delayed)              AS totaldelayed:long,
                  SUM(simple_flights.DepDelay)             AS totaldelay:long,
                  SUM(simple_flights.Cancelled)            AS totalcancelled:long,
                  SUM(simple_flights.Diverted)             AS totaldiverted:long,
                  SUM(simple_flights.weather_cancellation) AS total_weather_cancellation:long;

STORE summed into '$OUTPUT_PATH' USING PigStorage (';');
