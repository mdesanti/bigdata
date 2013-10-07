Para ejecutar los scripts de hive

hive -f script.hql

Los scripts aceptan el parámetro FLIGHT_DATA con la ubicacion de los archivos con los datos de los vuelos

hive -hiveconf FLIGHT_DATA='/user/hadoop/ITBA/TP1/INPUT/SAMPLE/data' -f script.hql

Adicionalmente, el script metric9.hql acepta AIRPORTS_DATA, con la ubicacion del archivo con los datos de los aeropuertos

Los output se guardan en el HDFS en el siguiente path
/user/hadoop/output/metricX

donde X es el nro de la métrica
