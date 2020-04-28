#!/bin/bash

# IMPORT CONFIG FILE FOR OVERALL LOAD PROCESS
source /home/o2072/scripts/config_osx.sh

# GENERATE TICEKT FOR HDFS ACCESS
kinit -kt /home/o2072/o2072.keytab o2072@FGB.AE

# BELOW DEFINE ALL CUSTOM VARIABLES USED
flagDateFormat=0
flagDateValid=0
month=`date -d $1 '+%m'`
day=`date -d $1 '+%e'`
year=`date -d $1 '+%Y'`
lastDay=`cal $month $year | grep -v "^$" | tail -1 | awk '{print $NF}'`
timestamp=`date +%Y%m%d%H%M%S`


# BELOW DEFINE ALL THE REQUIRED FUNCTION USED
getDate() {
   printf -v date '%(%Y%m%d)T' -1
   echo $date
}

getDateTime(){
   # PUT THE CURRENT DATE TIME IN YYYY-MM-DD HH:MM:SS FORMAT AND HOLD IT INSIDE VARIABLE - datetime
   printf -v datetime '%(%Y-%m-%d %H:%M:%S)T\n' -1
   echo $datetime
}

isValidFormat() {
   if [[ $1 =~ ^[0-9]{4}[0-9]{2}[0-9]{2}$ ]]
      then flagDateFormat=1
      else flagDateFormat=0
   fi
}

isValidDate() {
   local d="$1"
   date "+%Y%m%d" -d "$d" > /dev/null 2>&1

   if [ $? != 0 ]
   then
       flagDateValid=0
   else
       flagDateValid=1
   fi
}

isValidFormat $1
isValidDate $1

# ASSIGN LOG FILE PATH BASED ON DAILY LOAD OR MONTHLY LOAD
if [[ $day == $lastDay ]]
   then  logFilePath=$logPathMonthly'refresh_data_'$timestamp'_'$1'.log'
   else  logFilePath=$logPathDaily'refresh_data_'$timestamp'_'$1'.log'
fi
logFilePathSqoop=$logPathSqoop'refresh_param_'$timestamp'_'$1'.log'

export logFilePath
export logFilePathSqoop

# VALIDATE TIMEKEY FORMAT AND EXIT IF IT'S NOT CORRECT
if [[ $flagDateFormat == 1 &&  $flagDateValid == 1 ]]
  then echo $(getDateTime) "DATA REFRESH INTIATED FOR TIMEKEY - "$1 >> $logFilePath
  else
       echo $(getDateTime) "INVALID DATE/FORMAT. ACCEPTED FORMAT (YYYYMMDD)" >> $logFilePath
       exit 1
fi

# CALL INITIAL SCRIPT TO MOVE THE FILES FROM SUMX LOCAL PATH TO HDFS
$scriptPathPreProcess >> $logFilePath

# CALL DIMENSION LOAD SCRIPT TO TRIGGER REFRESH IN PARALLEL
$scriptPathDim $1

# CALL FACT LOAD SCRIPT TO PROCESS ALL THE FACT TABLES
$scriptPathFact $1

# CALL POST LOAD SCRIPT FOR CLEANUP AND FILE MOVEMENT TO PROPER DAILY AND MONTHLY FOLDERS
#$scriptPathPostProcess >> $logFilePath
