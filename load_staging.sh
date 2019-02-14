#!/bin/bash

if [ $# != 2 ]
   then
     echo "Usage: $0 files_to_download staging_hdfs_path"
     echo "   Note:  staging_hdfs_path is the folder in HDFS where you want to store the raw files. Do not include the trailing slash in  staging_hdfs_path."  
     echo "          files_to_download is a text file with a URL on each line. For example: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.csv"
     exit 1
fi

FILES_TO_DOWNLOAD=$1
STAGING_HDFS_PATH=$2

read -r -p "Confirm the following: Text file with list of raw files to download: $FILES_TO_DOWNLOAD, HDFS Path without a trailing slash: $STAGIN_HDFS_PATH [Y/n]" response
case "$response" in
     [yY][eE][sS]|[yY])
	echo "starting..."
	;;
     *)
	echo "Exiting"
	exit 1
	;;
esac


echo "Creating external hive table..."
hive -f ./sql/load_staging.sql --hiveconf staging_hdfs_path="$STAGING_HDFS_PATH"
echo "Done creating external hive table."

while read f;
do
	filename=${f##*/}
	echo "Downloading $filename..."
	wget $f
	echo "Done downloading."
	echo "Extracting year and month from filename..."
	year=$(echo "$f" | grep -oP '[\d]+-[\d]+' | awk -F '[-]' '{print $1}')
    	month=$(echo "$f" | grep -oP '[\d]+-[\d]+' | awk -F '[-]' '{print $2}')
    	echo "Year: $year, Month: $month"
	echo "Compressing..."
	tar -czvf $filename.tar.gz $filename
	echo "Done Compressing."
	echo "Putting to HDFS..."
	hdfs dfs -mkdir -p "$STAGING_HDFS_PATH/$year/$month"
	hdfs dfs -put $filename.tar.gz $STAGING_HDFS_PATH/$year/$month/$filename.tar.gz
	echo "Done Putting to HDFS."
	rm $filename
	rm $filename.tar.gz
	echo "Done Processing $filename"
	echo "Creating partition for year, month in the external table."
	hive -e "ALTER TABLE taxi.taxi_trips_raw ADD PARTITION (year='$year', month='$month') LOCATION '$STAGING_HDFS_PATH/$year/$month';"
	echo "Done creating partition."
done < $1

