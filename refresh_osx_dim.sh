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
flagCurrencyFile=0
flagCustomerFile=0
flagContractFile=0
flagStructureFile=0
flagAccountFile=0
flagPCFile=0

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

checkDimensionFilesAvailable(){
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_CURRENCY_CODE_FGB.DAT')
      then flagCurrencyFile=1
      else echo $(getDateTime) 'CURRENCY FILE NOT AVAILABLE. ABORTING LOAD PROCESS' >> $logFilePath
           exit 2
      fi
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_STRUCTURE_DIMENSION_FGB.DAT')
      then flagStructureFile=1
      else echo $(getDateTime) 'STRUCTURE FILE NOT AVAILABLE. ABORTING LOAD PROCESS' >> $logFilePath
           exit 3
      fi
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_ACCOUNT_CODE_FGB.DAT')
      then flagAccountFile=1
      else echo $(getDateTime) 'ACCOUNT CODE FILE NOT AVAILABLE. ABORTING LOAD PROCESS' >> $logFilePath
           exit 4
      fi
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_PROFIT_CENTRE_FGB.DAT')
      then flagPCFile=1
      else echo $(getDateTime) 'PROFIT CENTRE FILE NOT AVAILABLE. ABORTING LOAD PROCESS' >> $logFilePath
           exit 5
      fi
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_TRN_CONTRACT_FGB.DAT')
      then flagContractFile=1
      else flagContractFile=0
      fi
   if $(hadoop fs -test -f $hdfsPathDaily'T_OUTBOUND_CUSTOMER_FGB.DAT')
      then flagCustomerFile=1
      else flagCustomerFile=0
      fi
   
   if [[ $flagCurrencyFile == 1 && $flagStructureFile == 1 && $flagAccountFile == 1 && $flagPCFile == 1 && $flagContractFile == 1 && $flagCustomerFile == 1 ]]
        then echo $(getDateTime) 'ALL DIMENSION FILES AVAILABLE. PROCEEDING LOAD' >> $logFilePath
   elif [[ $flagCustomerFile == 0  ]]
        then echo $(getDateTime) 'CUSTOMER FILE NOT AVAILABLE. PROCEEDING WITH LOAD' >> $logFilePath 
   elif [[ $flagContractFile == 0  ]]
        then echo $(getDateTime) 'CONTRACT FILE NOT AVAILABLE. PROCEEDING WITH LOAD' >> $logFilePath
   fi
}

isValidFormat $1
isValidDate $1

# ASSIGN LOP FILE PATH FOR PARAMETER TABLES SQOOP LOAD
if [[ ${#logFilePathSqoop} == 0 ]]
then logFilePathSqoop=$logPathSqoop'refresh_param_'$timestamp'_'$1'.log'
     export logFilePathSqoop
fi

# ASSIGN LOG FILE PATH BASED ON DAILY LOAD OR MONTHLY LOAD ONLY WHEN THE SCRIPT IS CALLED DIRECTLY
if [[ ${#logFilePath} == 0 ]]; then
   if  [[ $day == $lastDay ]]
        then  logFilePath=$logPathMonthly'refresh_dim_'$timestamp'_'$1'.log'
        else  logFilePath=$logPathDaily'refresh_dim_'$timestamp'_'$1'.log'
   fi
   export logFilePath
fi

# CHECK VALIDITY OF DATE PASSED
if [[ $flagDateFormat == 1 &&  $flagDateValid == 1 ]]
  then echo $(getDateTime) "DIMENSION REFRESH ENTRY FOR TIMEKEY - "$1 >> $logFilePath
  else
       echo $(getDateTime) "INVALID DATE/FORMAT. ACCEPTED FORMAT (YYYYMMDD). SCRIPT EXITED" >> $logFilePath
       echo $(getDateTime) "INVALID DATE/FORMAT. ACCEPTED FORMAT (YYYYMMDD). SCRIPT EXITED" 
       exit 1
fi

# CHECK IF ALL THE DIMENSION FILES AVAILABLE OR NOT BEFORE PROCEEDING TO DIMENSION LOAD
checkDimensionFilesAvailable

# CALL FURTHER PROCESS TO LOAD DATA
echo $(getDateTime) 'DIMENSION REFRESH INITIATED IN PARALLEL' >> $logFilePath

if [[ $flagCustomerFile == 1 && $flagContractFile == 1 ]]
   then $scriptPathDimCus & $scriptPathDimCon & $scriptPathDimOth
elif [[ $flagCustomerFile == 1 ]]
   then $scriptPathDimCus & $scriptPathDimOth
elif [[ $flagContractFile == 1 ]]
   then $scriptPathDimCon & $scriptPathDimOth
else $scriptPathDimOth
fi
wait

impala-shell -i dev1node01.fgb.ae:25003 -f $scriptPathImpDim >> $logFilePath
echo $(getDateTime) 'DIMENSION TABLES REFRESHED IN IMPALA' >> $logFilePath
echo $(getDateTime) 'DIMENSION REFRESH COMPLETED' >> $logFilePath
