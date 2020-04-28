#!/bin/bash

# IMPORT CONFIG FILE FOR OVERALL LOAD PROCESS
source /home/o2072/scripts/config_osx.sh

getDateTime(){
   # PUT THE CURRENT DATE TIME IN YYYY-MM-DD HH:MM:SS FORMAT AND HOLD IT INSIDE VARIABLE - datetime
   printf -v datetime '%(%Y-%m-%d %H:%M:%S)T\n' -1
   echo $datetime
}

echo $(getDateTime) 'FILE MOVEMENT TO HDFS FROM LOCAL'

for file in $OT;
do
if [[ $file = *T_OUTBOUND_ACCOUNT_CODE_EXTENSION_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_ACCOUNT_CODE_EXTENSION_FGB.DAT'
elif [[ $file = *T_OUTBOUND_ACCOUNT_CODE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_ACCOUNT_CODE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_CURRENCY_CODE_FGB* ]]
then hadoop fs -put -f  $file $outHDFS'T_OUTBOUND_CURRENCY_CODE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_CUSTOMER_FGB* ]]
then hadoop fs -put -f  $file $outHDFS'T_OUTBOUND_CUSTOMER_FGB.DAT'
elif [[ $file = *T_OUTBOUND_DEPARTMENT_DERIVATION_FGB* ]]
then hadoop fs -put -f  $file $outHDFS'T_OUTBOUND_DEPARTMENT_DERIVATION_FGB.DAT'
elif [[ $file = *T_OUTBOUND_ENTITY_FGB* ]]
then hadoop fs -put -f  $file $outHDFS'T_OUTBOUND_ENTITY_FGB.DAT'
elif [[ $file = *T_OUTBOUND_DOMAIN_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_DOMAIN_FGB.DAT'
elif [[ $file = *T_OUTBOUND_GL_CODE_PRODUCT_TYPE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_GL_CODE_PRODUCT_TYPE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_MPG_RC_CODE_PROFIT_CENTRE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_MPG_RC_CODE_PROFIT_CENTRE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_BOOK_CODE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_BOOK_CODE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_DEAL_TYPE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_DEAL_TYPE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_PROFIT_CENTRE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_MPG_SOURCE_ONSX_VALUE_PROFIT_CENTRE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_PROFIT_CENTRE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_PROFIT_CENTRE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_TRN_CONTRACT_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_TRN_CONTRACT_FGB.DAT'
elif [[ $file = *T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_ENTITY_ACCOUNT_CODE_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_ENTITY_ACCOUNT_CODE_FGB.DAT'
elif [[ $file = *T_OUTBOUND_STRUCTURE_DIMENSION_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_STRUCTURE_DIMENSION_FGB.DAT'
elif [[ $file = *T_OUTBOUND_DEPARTMENT_DERIVATION_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_DEPARTMENT_DERIVATION_FGB.DAT'
elif [[ $file = *T_OUTBOUND_AMOUT_CLASS_FGB* ]]
then hadoop fs -put -f $file $outHDFS'T_OUTBOUND_AMOUT_CLASS_FGB.DAT'
elif [[ $file = *T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB* ]]
then tk=`echo ${file#*LOOP_}|cut -c 1-8`
     hadoop fs -put -f $file $outHDFS'T_OUTBOUND_DEAL_BALANCE_AVERAGE_BALANCE_FGB_'$tk'.DAT'
fi
done

echo $(getDateTime) 'FILE MOVEMENT TO HDFS COMPLETED'
