#!/bin/bash
# BELOW DEFINE ALL ESSENTIAL SCRIPT PATHS USED
scriptPathDimOth="/home/o2072/scripts/refresh_osx_dim_others.sh"
scriptPathDimCus="/home/o2072/scripts/refresh_osx_dim_customer.sh"
scriptPathDimCon="/home/o2072/scripts/refresh_osx_dim_contracts.sh"
scriptPathAgg="/home/o2072/scripts/refresh_osx_agg.sh"
scriptPathImpDim="/home/o2072/scripts/refresh_impala_dim.sql"
scriptPathImpBase="/home/o2072/scripts/refresh_impala_base_agg.sql"
scriptPathImpCustom="/home/o2072/scripts/refresh_impala_custom_agg.sql"

scriptPathPreProcess="/home/o2072/scripts/move_file_pre_process.sh"
scriptPathPostProcess="/home/o2072/scripts/move_file_post_process.sh"
scriptPathDim="/home/o2072/scripts/refresh_osx_dim.sh"
scriptPathFact="/home/o2072/scrits/refresh_osx_fact.sh"

# BELOW DEFINE ALL LOG FILE PATHS USED
logPathDaily="/home/o2072/logs/logs_daily/"
logPathMonthly="/home/o2072/logs/logs_monthly/"
logPathSqoop="/home/o2072/logs/logs_sqoop/"

# BELOW DEFINE ANY HDFS PATHS USED
hdfsPathDaily="/raw/onesumx/daily/"
hdfsPathBudget="/raw/onesumx/budget/"
outFile="/home/o2072/SUMX_Files/*.DAT"
outLocal="/home/o2072/SUMX_Files/"
outHDFS="/raw/onesumx/daily/"
OT=$outFile
