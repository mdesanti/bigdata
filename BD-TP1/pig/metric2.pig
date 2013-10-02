REGISTER ./pig-0.11.1/contrib/piggybank/java/piggybank.jar;

flights = LOAD '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data/'
          USING org.apache.pig.piggybank.storage.CSVLoader() 
          AS (Year:chararray, Month:chararray, DayofMonth:chararray, DayOfWeek:chararray, 
              DepTime:chararray, CRSDepTime:chararray, ArrTime:chararray, CRSArrTime:chararray,
              UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,  ActualElapsedTime:chararray,
              CRSElapsedTime:chararray, AirTime:chararray, ArrDelay:chararray, DepDelay:long, origin:chararray,
              Dest:chararray, Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int,
              CancellationCode:chararray, Diverted:chararray, CarrierDelay:chararray, WeatherDelay:chararray,
              NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

airports = LOAD '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/ref/airports.csv' 
           USING org.apache.pig.piggybank.storage.CSVLoader() 
           AS (id:chararray, airport:chararray);
/* We want for each day the amount of delayed planes, the amount of cancelled, the amount of diverted and the amount of diverted by weather.
   When we generate simple_flights, we map the departure delay to a 1 or a 0 to make the addition easier (same with cancellation code)
*/
simple_flights = FOREACH flights
                 GENERATE CONCAT((chararray)DayofMonth, CONCAT('/', CONCAT((chararray)Month, CONCAT('/', (chararray)Year)))) AS day:chararray,
                 (DepDelay > 0 ? 1:0) as delayed:long, DepDelay, Cancelled, Diverted,
                 (CancellationCode == 'B' ? 1:0) AS weather_cancellation:long;

grouped = GROUP simple_flights BY (day);

summed = FOREACH grouped
         GENERATE simple_flights.day, SUM(simple_flights.delayed) AS totaldelayed:long, SUM(simple_flights.DepDelay) AS totaldelay:long,
          SUM(simple_flights.Cancelled) AS totalcancelled:long, SUM(simple_flights.Diverted) AS totaldiverted:long, 
          SUM(simple_flights.weather_cancellation) AS total_weather_cancellation:long;


results = FOREACH summed { 
  generate group, flatten(summed);
};

simple_results = FOREACH results GENERATE Year, airport, totaldelay;

STORE simple_results into 'top5/pig_output' USING PigStorage (';');
