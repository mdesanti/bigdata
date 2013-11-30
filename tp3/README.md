# TP3

This project is a Rails application. When running, enter to http://localhost:3000 and there you will find the graphs.


To migrate data from HDFS to mysql, just run this command:

```
sqoop-export --connect jdbc:mysql://54.224.21.206:3306/itba_pintos --username pintos --password 5678 --table flown_miles --export-dir /user/hadoop/LUCILA/m3/part-r-00000 --columns "year, airline_code, miles, airline_code2, airline_name" --fields-terminated-by "\t"
```
