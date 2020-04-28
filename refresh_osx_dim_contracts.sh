getDateTime() {
   # PUT THE CURRENT DATE TIME IN YYYY-MM-DD HH:MM:SS FORMAT AND HOLD IT INSIDE VARIABLE - datetime
   printf -v datetime '%(%Y-%m-%d %H:%M:%S)T\n' -1
   echo $datetime
}

echo $(getDateTime) 'OSX CONTRACTS DIMENSION SCRIPT STARTED' >> $logFilePath

sh spark-submit \
--class load_dimension.load_osx_contracts \
--master yarn \
--deploy-mode cluster \
--driver-memory 20G \
--num-executors 10 \
--executor-memory 5G \
--executor-cores 3 \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.yarn.maxAppAttempts=1 \
/home/o2072/jars/execute_sumx_load_dim.jar

echo $(getDateTime) 'OSX CONTRACTS DIMENSION SCRIPT COMPLETED' >> $logFilePath
