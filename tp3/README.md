# TP3

This project is a Rails application. When running, enter to http://localhost:3000 and there you will find the graphs.


To migrate data from HDFS to mysql, just run this command:

```
sqoop-export --connect jdbc:mysql://10.212.83.136:3306/TEXAS --username root --password root --table sqoop_practice --export-dir /user/hadoop/LUCILA/m9/part-r-00000 --columns "year, origin, dest, qty" --fields-terminated-by "\t"
```
