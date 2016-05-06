#!/bin/bash
cnt=$1
a=0
cntr=1
while [ $a -lt ${cnt} ]
do
   if [ $a -eq ${cnt} ]
   then
      break
   fi
   a=`expr $a + 1`
   cntr=`expr $cntr + 1`
   product=$cntr
   customer=$cntr
   qty=`expr $cntr + 2`
   price=`expr $cntr + 10`
   amt=`expr $qty  \* $price  `
   sleep 2
   dt=`date +"%Y%m%d"`
   timestamp=`date +"%s"`
   #echo `date`"|"${dt}"|"${timestamp}"|"${product}"|"${customer}"|"${qty} >> log.txt
   echo `date`"|"${dt}"|"${timestamp}"|"${product}"|"${customer}"|"${qty}"|"$price"|"$amt.99 >> order.log
   if [ $cntr == 10 ]
   then
       cntr=0
   fi
done

