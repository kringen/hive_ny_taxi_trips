USE taxi;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS taxi_trips
(
	VendorID INT,
	pickup_timestamp TIMESTAMP,
	dropoff_timestamp TIMESTAMP,
	Passenger_count INT,
	Trip_distance FLOAT,
	PULocationID INT,
	DOLocationID INt,
	RateCodeID INT,
	Store_and_fwd_flag STRING,
	Payment_type INT,
	Fare_amount FLOAT,
	Extra FLOAT,
	MTA_tax FLOAT,
	Improvement_surcharge FLOAT,
	Tip_amount FLOAT,
	Tolls_amount FLOAT,
	Total_amount FLOAT
)
PARTITIONED BY (year STRING, month STRING, pickup_date DATE)
CLUSTERED BY (PULocationID) INTO 263 BUCKETS;

INSERT OVERWRITE TABLE taxi_trips PARTITION(year, month, pickup_date)
SELECT
	VendorID,
	FROM_UNIXTIME(UNIX_TIMESTAMP(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')) AS pickup_timestamp,
        FROM_UNIXTIME(UNIX_TIMESTAMP(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')) AS dropoff_timestamp,
	Passenger_count,
        Trip_distance,
        PULocationID,
        DOLocationID,
        RateCodeID,
        Store_and_fwd_flag,
        Payment_type,
        Fare_amount,
        Extra,
        MTA_tax,
        Improvement_surcharge,
        Tip_amount,
        Tolls_amount,
        Total_amount,
	`year`,
	`month`,
	FROM_UNIXTIME(UNIX_TIMESTAMP(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd") AS pickup_date
FROM taxi_trips_raw
WHERE year = ${hiveconf:year}
AND month = ${hiveconf:month}
