getDateTime() {
   # PUT THE CURRENT DATE TIME IN YYYY-MM-DD HH:MM:SS FORMAT AND HOLD IT INSIDE VARIABLE - datetime
   printf -v datetime '%(%Y-%m-%d %H:%M:%S)T\n' -1
   echo $datetime
}

echo $logFilePath
echo $logFilePathSqoop
echo $(getDateTime) 'OTHER DIMENSION LOAD INITIATED' >> $logFilePath

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:oracle:thin:@172.20.37.173:1526:datamart02 \
--username alchemy02 \
--password alchemy02 \
--table DIM_CONSTANTS \
--delete-target-dir \
--target-dir /data/fin_onesumx/dim_constants \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--num-mappers 1 \
--split-by 1 \
--as-parquetfile 2>&1| tee -a $logFilePathSqoop

echo  $(getDateTime) 'DIM_CONSTANT LOADED FROM ALCHEMY' >> $logFilePath 

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:oracle:thin:@172.20.37.173:1526:datamart02 \
--username alchemy02 \
--password alchemy02 \
--table PBI_DATAFLOW_TBL \
--delete-target-dir \
--target-dir /data/fin_onesumx/pbi_dataflow_tbl \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--num-mappers 1 \
--split-by 1 \
--as-parquetfile 2>&1| tee -a $logFilePathSqoop

echo  $(getDateTime) 'PBI_DATAFLOW_TABLE LOADED FROM ALCHEMY' >> $logFilePath

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:oracle:thin:@172.20.37.173:1526:datamart02 \
--username alchemy02 \
--password alchemy02 \
--table PARAMETER_TBL \
--delete-target-dir \
--target-dir /data/fin_onesumx/parameter_tbl \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--num-mappers 1 \
--split-by 1 \
--as-parquetfile 2>&1| tee -a $logFilePathSqoop


echo $(getDateTime) 'PARAMETER_TBL LOADED FROM ALCHEMY' >> $logFilePath

sh spark-submit \
--class load_dimension.load_osx_other_dim \
--master yarn \
--deploy-mode cluster \
--driver-memory 10G \
--num-executors 2 \
--executor-memory 2G \
--executor-cores 2 \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=2 \
--conf spark.yarn.maxAppAttempts=1 \
/home/o2072/jars/execute_sumx_load_dim.jar >> $logFilePathSqoop

echo $(getDateTime) 'OTHER DIMENSIONS LOADED FROM SPARK SUBMIT' >> $logFilePath
