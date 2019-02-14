CREATE DATABASE IF NOT EXISTS taxi;

USE taxi;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips_raw2
(
	vendorid INT,
	tpep_pickup_datetime STRING,
	tpep_dropoff_datetime STRING,
	Passenger_count INT,
	Trip_distance FLOAT,
	RateCodeID INT,
	Store_and_fwd_flag STRING,
	PULocationID INT,
	DOLocationID INT,
	Payment_type INT,
	Fare_amount FLOAT,
	Extra FLOAT,
	mta_tax FLOAT,
	tip_amount FLOAT,
	tolls_amount FLOAT,
	Improvement_surcharge FLOAT,
	Total_amount FLOAT
)
PARTITIONED BY (year STRING, month STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:staging_hdfs_path}'
TBLPROPERTIES ("skip.header.line.count"="1");
