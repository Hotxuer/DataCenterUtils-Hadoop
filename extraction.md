## mysql导入HIVE

创建同结构的hive表
```bash
hive> CREATE TABLE train (
  id int,
  train string,
  weight int,
  goods int,
  start_station int,
  end_station int,
  train_time timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
```

sqoop导入
```bash
$ sqoop import \
--connect jdbc:mysql://localhost:3306/hue_pre \
--username hue -P \
--table train_test \
--fields-terminated-by ',' \
--delete-target-dir \
--num-mappers 1 \
--hive-import \
--hive-database test1 \
--hive-table train
```
注意sqoop命令中的分隔符应与HIVE建表时设定的分隔符相一致

## csv导入HIVE

先在HIVE中创建临时表train_raw以进行后续的日期处理
```bash
hive> CREATE TABLE train_raw (
  train string,
  weight string,
  goods string,
  start_station string,
  end_station string,
  year string,
  month string,
  day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
```

使用LOAD命令将csv数据导入临时表
```bash
hive> LOAD DATA local INPATH '/home/hadoop/DBGenerator/train_100.csv' into table test1.train_raw;
```

创建sql脚本transform.sql处理日期格式，进行导入（已放入131相应目录）
```bash
-- transform.sql
-- hive无自增主键功能，此处先把id设为0
insert into train select
    0,
    train,
    weight,
    goods,
    start_station,
    end_station,
    cast (concat(substr(year, 2), '-', substr(month, 2), '-', substr(day, 2), ' 00:00:00') as timestamp)
from train_raw;
```

执行脚本
```bash
hive> source /home/hadoop/DBGenerator/transform.sql;
```