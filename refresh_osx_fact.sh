#!/bin/bash
# IMPORT CONFIG FILE FOR OVERALL LOAD PROCESS
source /home/o2072/scripts/config_osx.sh

# BELOW DEFINE ALL CUSTOM VARIABLES USED
flagDateFormat=0
flagDateValid=0
month=`date -d $1 '+%m'`
day=`date -d $1 '+%e'`
year=`date -d $1 '+%Y'`
lastDay=`cal $month $year | grep -v "^$" | tail -1 | awk '{print $NF}'`
timestamp=`date +%Y%m%d%H%M%S`
availableFlag=0

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

checkFactFileAvailable() {
   for fileName in `hadoop fs -ls /raw/onesumx/daily | awk '{print $NF}' | grep .DAT$`
   do
      name=`echo $fileName | cut -d'/' -f 5`
      if [[ $name = 'T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB_'$1'.DAT' ]]
         then availableFlag=1
              echo $(getDateTime) 'BALANCE FILE AVAILABLE FOR THE REQUESTED TIMEKEY. PROCEEDING LOAD.' >> $logFilePath
              break
      fi
   done
   if [[ $availableFlag != 1 ]]
   then echo 'BALANCE FILE NOT AVAILABLE. ABORTING PROCESS' >> $logFilePath
        echo 'BALANCE FILE NOT AVAILABLE. ABORTING PROCESS'
        exit 1
   fi
}

# ASSIGN LOG FILE PATH BASED ON DAILY LOAD OR MONTHLY LOAD ONLY WHEN THE SCRIPT IS CALLED DIRECTLY
if [[ ${#logFilePath} == 0 ]]; then
   if  [[ $day == $lastDay ]]
        then  logFilePath=$logPathMonthly'refresh_fact_'$timestamp'_'$1'.log'
        else  logFilePath=$logPathDaily'refresh_fact_'$timestamp'_'$1'.log'
   fi
   export logFilePath
fi

# CHECK VALIDITY OF DATE PASSED
isValidFormat $1
isValidDate $1
if [[ $flagDateFormat == 1 &&  $flagDateValid == 1 ]]
  then echo $(getDateTime) "FACT REFRESH ENTRY FOR TIMEKEY - "$1 >> $logFilePath
  else 
       echo $(getDateTime) "INVALID DATE/FORMAT. ACCEPTED FORMAT (YYYYMMDD)" >> $logFilePath
       echo $(getDateTime) "INVALID DATE/FORMAT. ACCEPTED FORMAT (YYYYMMDD)"
       exit 1
fi

# CHECK IF THE REQUIRED BALANCE FILE TO BE PROCESSES IS AVAILABLE IN HDFS OR NOT
checkFactFileAvailable $1

# IF THE DATE IS A MONTH END DATE THEN TRIGGER BASE FIRST --> REFRESH IMPALA TABLES --> TRIGGER SSAS CUBE REFRESH SCRIPT --> TRIGGER REST CUSTOM TABLE LOADS
# IF THE DATE IS NOT MONTH END THEN TRIGGER ALL THE AGGREGATE LOAD TOGETHER --> REFRESH IMPALA TABLES
# AGGREGATE LOAD SCRIPT (refresh_osx_agg.sh) ACCEPTS TWO PARAMETERS : TIME_KEY, LOAD_PARAM
# TIME_KEY = YYYYMMDD FORMAT (EX- 20191231)
# LOAD_PARAM = EITHER OF THESE FOUR VALUES : BASE, REST, CUST, GL, ALL
# BASE - ONLY BASE AGG + TRIAL BALANCE WILL BE LOADED
# REST - LOADS CUST AND GL AGG TOGETHER [MONTH END SCENARIOS WHERE BASE GOES FIRST THEN REST]
# CUST - LOADS CUST AGGREGATE ONLY
# GL   - LOADS GL AGGREGATE ONLY
# ALL  - LOADS ALL THE AGGREGATES TOGETHER

if [[ $day == $lastDay ]]
then 
     echo $(getDateTime) 'AGGREGATE REFRESH INTIATED FOR MONTH END SCENARIO' >> $logFilePath
     echo $(getDateTime) 'BASE AGGREGATES REFRESH TRIGGERED' >> $logFilePath
     $scriptPathAgg $1 'BASE'  >> $logFilePath
     impala-shell -i dev1node01.fgb.ae:25003 -f $scriptPathImpBase --var tk=$1 >> $logFilePath
     echo $(getDateTime) 'BASE AGGREGATES REFRESHED IN IMPALA' >> $logFilePath
     echo $(getDateTime) 'SSAS CUBE REFRESH SCRIPT TRIGGERED' >> $logFilePath
     echo $(getDateTime) 'REST CUSTOM AGGREGATES REFRESH TRIGGERED' >> $logFilePath
     $scriptPathAgg $1 'REST'  >> $logFilePath
     impala-shell -i dev1node01.fgb.ae:25003 -f $scriptPathImpCustom --var tk=$1 >> $logFilePath
     echo $(getDateTime) 'REST CUSTOM AGGREGATES REFRESHED IN IMPALA' >> $logFilePath
     echo $(getDateTime) 'MONTH END LOAD COMMENCES' >> $logFilePath
else 
     echo $(getDateTime) 'AGGREGATE REFRESH INTIATED FOR DAILY LOAD SCENARIO' >> $logFilePath
     echo $(getDateTime) 'ALL AGGREGATES REFRESH TRIGGERED' >> $logFilePath
     $scriptPathAgg $1 'ALL'  >> $logFilePath
     impala-shell -i dev1node01.fgb.ae:25003 -f $scriptPathImpBase --var tk=$1 >> $logFilePath
     impala-shell -i dev1node01.fgb.ae:25003 -f $scriptPathImpCustom --var tk=$1 >> $logFilePath
     echo 'ALL AGGREGATES REFRESHED IN IMPALA' >> $logFilePath
     echo 'DAILY LOAD COMMENCES' >> $logFilePath
fi

echo $(getDateTime) 'FACT REFRESH COMPLETED' >> $logFilePath
