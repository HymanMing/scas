#! /bin/bash
CURRENT=`/bin/date +%y%m%d`

# clean hdfs data and output 
/opt/modules/hadoop-2.5.0/bin/hadoop jar smartcommunity-1.0.0.jar $CURRENT

# use hive to stats

## 1.location data to partition
/opt/modules/hive-0.13.1-bin/bin/hive -e "ALTER TABLE scas ADD PARTITION(logdate='$CURRENT') LOCATION '/home/scas/cleaned/$CURRENT';"

## 2.stats pv
/opt/modules/hive-0.13.1-bin/bin/hive -e "CREATE TABLE pv_$CURRENT AS SELECT COUNT(1) AS PV FROM scas WHERE logdate='$CURRENT';"

## 3.stats ip
/opt/modules/hive-0.13.1-bin/bin/hive -e "CREATE TABLE ip_$CURRENT AS SELECT COUNT(DISTINCT ip) AS IP FROM scas WHERE logdate='$CURRENT';"

## 4.stats amount hour
/opt/modules/hive-0.13.1-bin/bin/hive -e "CREATE TABLE amount_$CURRENT ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT '$CURRENT',hour AS HOUR_TAG, COUNT(hour) AS HOUR,'' AS UPDATE_DATE FROM scas WHERE logdate='$CURRENT' GROUP BY hour;"

## 5.stats jr
/opt/modules/hive-0.13.1-bin/bin/hive -e "CREATE TABLE jr_$CURRENT AS SELECT COUNT(1) AS JR FROM (SELECT COUNT(ip) AS times FROM scas WHERE logdate='$CURRENT' GROUP BY ip HAVING times=1) e;"

## 6.combine pv,ip,jr and tr to scas table
/opt/modules/hive-0.13.1-bin/bin/hive -e "CREATE TABLE scas_$CURRENT ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT '$CURRENT', a.pv, b.ip, c.jr, ROUND(COALESCE(CAST(b.ip AS DOUBLE), 0)/a.pv, 2),'' AS UPDATE_DATE FROM pv_$CURRENT a JOIN ip_$CURRENT b ON 1=1 JOIN jr_$CURRENT c ON 1=1 ;"

# sqoop data to mysql

## 1.sqoop t_kpi_day
/opt/modules/sqoop-1.4.5/bin/sqoop export -D sqoop.export.records.per.statement=100 --connect jdbc:mysql://10.1.16.140:3306/scas --username root --password gmm123 --table t_kpi_day --fields-terminated-by ',' --export-dir "/home/hive/warehouse/scas_$CURRENT" --batch --update-key createdate --update-mode allowinsert;

## 2.sqoop t_kpi_hour
/opt/modules/sqoop-1.4.5/bin/sqoop export -D sqoop.export.records.per.statement=100 --connect jdbc:mysql://10.1.16.140:3306/scas --username root --password gmm123 --table t_kpi_hour --fields-terminated-by ',' --export-dir "/home/hive/warehouse/amount_$CURRENT" --batch --update-key createdate,kpi_code --update-mode allowinsert;

# drop tmp table to hive warehouse
/opt/modules/hive-0.13.1-bin/bin/hive -e "drop table amount_$CURRENT;drop table ip_$CURRENT;drop table jr_$CURRENT;drop table pv_$CURRENT;drop table scas_$CURRENT;"
