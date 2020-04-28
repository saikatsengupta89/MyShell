getDateTime() {
   # PUT THE CURRENT DATE TIME IN YYYY-MM-DD HH:MM:SS FORMAT AND HOLD IT INSIDE VARIABLE - datetime
   printf -v datetime '%(%Y-%m-%d %H:%M:%S)T\n' -1
   echo $datetime
}

echo $(getDateTime) 'AGGREGATE SCRIPT TRIGGERED - '$2 >> $logFilePath

sh spark-submit \
--master yarn \
--deploy-mode cluster \
--class sumx_aggregate.process_sumx_agg \
--driver-memory 20G \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 10G \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.yarn.maxAppAttempts=1 \
/home/o2072/jars/execute_sumx_load_agg.jar $1 $2 >> $logFilePath

echo $(getDateTime) 'AGGREGATE SCRIPT COMPLETED - '$2 >> $logFilePath
