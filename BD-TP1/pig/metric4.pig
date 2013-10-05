%default PIGGYBANK_PATH './pig-0.11.1/contrib/piggybank/java/piggybank.jar'
%default FLIGHTS_PATH '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data/'
%default AIRPORTS_HBASE_PATH 'hbase://itba_tp1_airports'
%default OUTPUT_PATH 'metric4/output'

REGISTER '$PIGGYBANK_PATH';

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

joined = JOIN flights BY origin, airports BY id;

simple_flights = FOREACH joined GENERATE ToDate( CONCAT(CONCAT((chararray)Year, '-'),
                                                  CONCAT(CONCAT((chararray)Month, '-'), (chararray)DayofMonth)), 'yyyy-M-d') as date:datetime,
                                                  airport, (CancellationCode == 'B' ? 1:0) as weather_cancel:int;

after_date = FOREACH simple_flights GENERATE ((date >= ToDate('2005-08-23', 'yyyy-MM-dd') and date <= ToDate('2005-08-30', 'yyyy-MM-dd'))? 'KATRINA' :
                                              ((date >= ToDate('1998-10-22', 'yyyy-MM-dd') and date <= ToDate('1998-11-05', 'yyyy-MM-dd'))? 'MITCH':
                                              (date >= ToDate('2005-10-15', 'yyyy-MM-dd') and date <= ToDate('2005-10-26', 'yyyy-MM-dd') ? 'WILMA' : 'NONE'))) as hurricane:chararray,
date, airport, weather_cancel;

grouped = GROUP after_date BY (hurricane, date);

summed = FOREACH grouped GENERATE group, SUM(after_date.weather_cancel) AS cancel:float;

results = FOREACH summed {
  sorted = ORDER cancel BY cancel desc;
  top_5 = LIMIT sorted 1;
  generate group, flatten(top_5);
};

STORE results into '$OUTPUT_PATH' USING PigStorage (';');
