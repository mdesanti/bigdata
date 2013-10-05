REGISTER ./pig-0.11.1/contrib/piggybank/java/piggybank.jar;


flights = LOAD '/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data/'
          USING org.apache.pig.piggybank.storage.CSVLoader()
          AS (Year:chararray, Month:chararray, DayofMonth:chararray, DayOfWeek:chararray,
              DepTime:chararray, CRSDepTime:chararray, ArrTime:chararray, CRSArrTime:chararray,
              UniqueCarrier:chararray, FlightNum:chararray, TailNum:chararray,  ActualElapsedTime:chararray,
              CRSElapsedTime:chararray, AirTime:chararray, ArrDelay:chararray, DepDelay:long, origin:chararray,
              Dest:chararray, Distance:chararray, TaxiIn:chararray, TaxiOut:chararray, Cancelled:int,
              CancellationCode:chararray, Diverted:int, CarrierDelay:chararray, WeatherDelay:chararray,
              NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

airports = LOAD 'hbase://itba_tp1_airports'
           USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:airport', '-loadKey true')
           AS (id:chararray, airport:chararray);

joined = JOIN flights BY origin, airports BY id;

simple_flights = FOREACH joined GENERATE ToDate( CONCAT(CONCAT((chararray)Year, '-'),
                                                  CONCAT(CONCAT((chararray)Month, '-'), (chararray)DayofMonth)), 'yyyy-M-d') as date:datetime,
                                                  airport, (DepDelay > 0 ? 1:0) as delayed:long;

after_date = FOREACH simple_flights GENERATE ((date >= ToDate('2005-08-23', 'yyyy-MM-dd') and date <= ToDate('2005-08-30', 'yyyy-MM-dd'))? 'KATRINA' :
                                              ((date >= ToDate('1998-10-22', 'yyyy-MM-dd') and date <= ToDate('1998-11-05', 'yyyy-MM-dd'))? 'MITCH':
                                              (date >= ToDate('2005-10-15', 'yyyy-MM-dd') and date <= ToDate('2005-10-26', 'yyyy-MM-dd') ? 'WILMA' : 'NONE'))) as hurricane:chararray,
                                              date, airport, delayed, ((date >= ToDate('2005-08-23', 'yyyy-MM-dd') and date <= ToDate('2005-08-30', 'yyyy-MM-dd'))? 8.0 :
                                              ((date >= ToDate('1998-10-22', 'yyyy-MM-dd') and date <= ToDate('1998-11-05', 'yyyy-MM-dd'))? 15.0:
                                              (date >= ToDate('2005-10-15', 'yyyy-MM-dd') and date <= ToDate('2005-10-26', 'yyyy-MM-dd') ? 12.0 : 1.0))) as days:double;

after_date = FILTER after_date BY hurricane == 'KATRINA' OR hurricane == 'MITCH' OR hurricane == 'WILMA';

grouped = GROUP after_date BY (hurricane, airport);

summed = FOREACH grouped GENERATE group, SUM(after_date.delayed)/MAX(after_date.days) AS avgdelay:float;

flat = FOREACH summed GENERATE group.hurricane, group.airport, flatten(avgdelay);

by_hurricane = GROUP flat BY hurricane;

results = FOREACH by_hurricane {
  sorted = ORDER flat BY avgdelay desc;
  top_5 = LIMIT sorted 5;
  generate group, flatten(top_5);
};
