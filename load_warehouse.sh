#!/bin/bash

#RAW="This is a/ test"
STAGING_PARTITIONS=$(hive -e "use taxi; show partitions taxi_trips_raw;" | grep -oP 'year=[\d]+/month=[\d]+')
DESTINATION_PARTITIONS=$(hive -e "use taxi; show partitions taxi_trips;")


for partition in $STAGING_PARTITIONS
do
	TEST_PARTITION=$(echo "$DESTINATION_PARTITIONS" | grep $partition)
	if [ -z "$TEST_PARTITION" ]
	then
       		echo "Partition $partition does not exist in destination table.  Loading..."
		YEAR=$(echo "$partition" | awk -F '[/]' '{print $1}' | awk -F '[=]' '{print $2}')
		MONTH=$(echo "$partition" | awk -F '[/]' '{print $2}' | awk -F '[=]' '{print $2}')
		echo "$YEAR"
		echo "$MONTH"
		hive -f ./sql/load_warehouse.sql --hiveconf year=$YEAR --hiveconf month=$MONTH
	else
       		echo "Partition $partition exists in destination table.  Skipping"
	fi
done

#TEST_PARTITION=$(echo "$TABLE" | grep $RAW)

#if [ -z "$TEST_PARTITION" ]
#then
#	echo "Partition $RAW does not exist in destination table.  Loading..."
#else
#	echo "Partition $RAW exists in destination table.  Skipping"
#fi
#echo $STAGING_PARTITION
#echo $TEST_PARTITION
#awk -F '[/]' '{print $1}'
