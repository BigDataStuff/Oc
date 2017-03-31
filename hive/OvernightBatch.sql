//------------------------------------------------------------------------------------------------
//--STEP#1. Create Flume Agent using hdfs sink RollSize 1000 record into 1 File.
//------------------------------------------------------------------------------------------------
FlumeOrderDailyA1.sources = s1
FlumeOrderDailyA1.channels = c1
FlumeOrderDailyA1.sinks = k1

# Define Source
FlumeOrderDailyA1.sources.s1.type = exec
FlumeOrderDailyA1.sources.s1.command = tail -F /home/poc/order_daily.log
FlumeOrderDailyA1.sources.s1.restart = true
FlumeOrderDailyA1.sources.s1.batchSize = 100

# HDFS sinks
FlumeOrderDailyA1.sinks.k1.type = hdfs
FlumeOrderDailyA1.sinks.k1.hdfs.fileType =DataStream
FlumeOrderDailyA1.sinks.k1.hdfs.path = hdfs://nn1.app.com:/user/oc/flume/logs/%Y%m%d/%H/
FlumeOrderDailyA1.sinks.k1.hdfs.filePrefix = order
FlumeOrderDailyA1.sinks.k1.hdfs.fileSuffix = .log
FlumeOrderDailyA1.sinks.k1.hdfs.inUsePrefix = .
FlumeOrderDailyA1.sinks.k1.hdfs.inUseSuffix = .inuse 
FlumeOrderDailyA1.sinks.k1.hdfs.rollInterval = 0
FlumeOrderDailyA1.sinks.k1.hdfs.rollSize = 0
FlumeOrderDailyA1.sinks.k1.hdfs.rollCount = 1000
FlumeOrderDailyA1.sinks.k1.hdfs.batchSize = 100
FlumeOrderDailyA1.sinks.k1.hdfs.idleTimeout = 10
FlumeOrderDailyA1.sinks.k1.hdfs.useLocalTimeStamp = true

# Use channel which buffers events in memory 
FlumeOrderDailyA1.channels.c1.type = memory
FlumeOrderDailyA1.channels.c1.capacity=10000
FlumeOrderDailyA1.channels.c1.transactionCapacity=1000

FlumeOrderDailyA1.sources.s1.channels = c1
FlumeOrderDailyA1.sinks.k1.channel = c1

------------------------------------------------------------------------------------------------
--STEP#2. Start Flume Agent in Background
------------------------------------------------------------------------------------------------
nohup flume-ng agent --conf /etc/flume/conf/ --name FlumeOrderDailyA1 --conf-file FlumeOrderDailyA1.conf &


------------------------------------------------------------------------------------------------
--STEP#3. Create Hive Script to Add Partition to External Table
------------------------------------------------------------------------------------------------
set hivevar:iyyyymmdd=20160517;
set hivevar:ihour=20;

--Load External Staging Table for a Day
--Drop Partition if Already Exist
ALTER TABLE oc.order_ext_flume DROP PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  );
--Create new Partition
ALTER TABLE oc.order_ext_flume ADD  PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  ) LOCATION '/user/oc/flume/logs/${iyyyymmdd}/${ihour}';


------------------------------------------------------------------------------------------------
--STEP#4. Load Hive Managed table from External table
------------------------------------------------------------------------------------------------
--To Enable Compression on Intermediate Data in Multistage MapReduce
--To Load Compress data final output of a MapReduce job
--Tuning Pararmeters

set hive.intermediate.compression.type=BLOCK;
set hive.exec.compress.intermediate=TRUE;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.exec.compress.output=TRUE;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.exec.parallel=true;
set mapreduce.job.reduces=2;
set mapred.job.reuse.jvm.num.tasks=4;

--Load Data Into Managed Partition PARQUET Snappy Compressed table
FROM oc.order_ext_flume stg
INSERT OVERWRITE TABLE oc.order_prqt PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  )
SELECT 
Date,     
Dt,         
TimeStamp,  
productid, 
customerid, 
qty,        
price,      
amt     
WHERE 1=1
   AND stg.yyyymmdd=${iyyyymmdd}
   AND stg.hour=${ihour}  
;

--Gather Stats
ANALYZE TABLE oc.order_prqt       PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  )   COMPUTE STATISTICS;
--Count
SELECT o.yyyymmdd AS dt,
       o.hour     AS hour,
       count(*)   AS cnt , 
       sum(o.qty) AS qty, 
       sum(o.amt) AS amt
  FROM oc.order_prqt o
 WHERE o.yyyymmdd=${iyyyymmdd}
   AND o.hour=${ihour} 
 GROUP BY yyyymmdd,hour;


------------------------------------------------------------------------------------------------
--STEP#5. Run Spark SQL on Hive External Tables and Save Summary Data
------------------------------------------------------------------------------------------------
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

val hiveCtx = new HiveContext(sc)
val eresult= hiveCtx.sql("select o.yyyymmdd as yyyymmdd,o.hour as hour, count(1) cnt, p.name as Name,sum(o.qty) as qty ,sum(o.amt) as amt 
                            from oc.product_prqt p 
                           RIGHT OUTER JOIN 
                                oc.order_prqt o 
                              ON ( p.id = o.productid) 
                           GROUP BY o.yyyymmdd,o.hour, p.name 
                           ORDER BY o.yyyymmdd,o.hour ,sum(o.qty) desc")
                     .cache()
eresult.write.mode(SaveMode.Overwrite).parquet("/user/oc/spark/orders/")
eresult.show(10000)
eresult.write.format("parquet").option("path","/user/oc/hive/mng/order_sum_prqt").mode(SaveMode.Overwrite).saveAsTable("oc.order_sum_prqt")

//--eresult.write.format("parquet").option("Path","/user/oc/hive/mng/order_sum_prqt").partitionBy("yyyymmdd","hour").mode(SaveMode.Overwrite).saveAsTable("oc.order_sum_prqt")


------------------------------------------------------------------------------------------------
--STEP#5. Scala Application to Run Spark SQL on Hive External Tables and Save Summary Data
------------------------------------------------------------------------------------------------
vi orderSum.scala

//--Spark Conf and Context
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//--Hive Related
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode


object OrderSum {
  def main(args: Array[String]){
  //Spark Context initalization
  val conf= new SparkConf().setAppName("Order Sum")
  val sc  = new SparkContext(conf)
  //Creating HiveContext
  val hiveCtx = new HiveContext(sc)
  val eresult= hiveCtx.sql("SELECT o.yyyymmdd as dt, o.hour as hour, count(1) as cnt, sum(o.qty) as qty, sum(o.amt) as amt,p.name as productname FROM oc.product_prqt p RIGHT OUTER JOIN oc.order_prqt o ON (p.id = o.productid) GROUP BY o.yyyymmdd, o.hour, p.name ORDER BY o.yyyymmdd, o.hour, sum(o.qty) DESC").cache()
  val noofrec = eresult.count()
  eresult.write.mode(SaveMode.Overwrite).parquet("/user/oc/spark/orders/")
  eresult.write.format("parquet").option("path","/user/oc/hive/mng/order_sum_prqt").mode(SaveMode.Overwrite).saveAsTable("oc.order_sum_prqt")

  println("----------------------------------------")
  println("Order Summary Stats:-")
  println("----------------------------------------")
  println("Total No of Records : %s".format(noofrec))
  println("----------------------------------------")
  }
}


vi OrderSum.sbt

name := "OrderSum"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.0"

libraryDependencies += "org.apache.hive" % "hive" % "1.1.0"

spark-submit --conf spark.root.logger=ERROR,console --class OrderSum --master local[2]   /home/poc/OrderSum/target/scala-2.10/ordersum_2.10-1.0.jar

spark-submit --conf spark.root.logger=ERROR,console --class OrderSum /home/poc/oc/oozie/wf1/ordersum_2.10-1.0.jar \
             --master yarn --deploy-mode "cluster" --driver-memory 500m --num-executors 8 --executor-memory 512m --executor-cores 1 


------------------------------------------------------------------------------------------------
--STEP#5B. Using SQLContext and DataFrame Scala Application to Run Spark SQL on Hive External Tables and Save Summary Data
------------------------------------------------------------------------------------------------
vi OrderSumPrqt.scala

//--Spark Conf and Context
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//--SQL Related
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object OrderSumPrqt {
  def main(args: Array[String]){
  //Spark Context initalization
  val conf= new SparkConf().setAppName("Order Sum Prqt")
  val sc  = new SparkContext(conf)
  //Creating SQLContext and DataFrames
  val sqlCtx = new SQLContext(sc)
  val ofile = sqlCtx.read.format("parquet").load("/user/oc/hive/mng/order_prqt_daily")
  val pfile  = sqlCtx.read.format("parquet").load("/user/oc/hive/mng/oc/product_prqt/")
  pfile.registerTempTable("products_df")
  ofile.registerTempTable("orders_df")
  val eresult = sqlCtx.sql("SELECT o.yyyymmdd as dt, o.hour as hour, count(1) as cnt, sum(o.qty) as qty, sum(o.amt) as amt,p.name as productname FROM products_df p RIGHT OUTER JOIN orders_df o ON (p.id = o.productid) GROUP BY o.yyyymmdd, o.hour, p.name ORDER BY o.yyyymmdd, o.hour, sum(o.qty) DESC").cache()
  val noofrec = eresult.count()
  eresult.write.mode(SaveMode.Overwrite).parquet("/user/oc/spark/orders_sum/")

  println("----------------------------------------")
  println("Order Summary Stats:-")
  println("----------------------------------------")
  println("Total No of Records : %s".format(noofrec))
  println("----------------------------------------")
  }
}


vi OrderSumPrqt.sbt

name := "OrderSumPrqt"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "1.5.0"



spark-submit --conf spark.root.logger=ERROR,console --class OrderSumPrqt --master local[2]   /home/poc/OrderSumPrqt/target/scala-2.10/ordersumprqt_2.10-1.0.jar

spark-submit --conf spark.root.logger=ERROR,console --class OrderSumPrqt /home/poc/oc/oozie/wf1/ordersumprqt_2.10-1.0.jar \
             --master yarn --deploy-mode "cluster" --driver-memory 500m --num-executors 4 --executor-memory 1g --executor-cores 1 


------------------------------------------------------------------------------------------------
--STEP#5c. Using SQLContext and DataFrame Scala Application to Run Spark SQL on Hive External Tables and Daily Data
------------------------------------------------------------------------------------------------
vi OrderSumPrqtDaily.scala

//--Spark Conf and Context
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//--SQL Related
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object OrderSumPrqtDaily {
  def main(args: Array[String]){
  //Spark Context initalization
  val conf= new SparkConf().setAppName("Order Sum Prqt")
  val sc  = new SparkContext(conf)
  //Creating SQLContext and DataFrames
  val sqlCtx = new SQLContext(sc)
  val ofile = sqlCtx.read.format("parquet").load("/user/oc/hive/mng/order_prqt_daily")
  val pfile  = sqlCtx.read.format("parquet").load("/user/oc/hive/mng/oc/product_prqt/")
  pfile.registerTempTable("products_df")
  ofile.registerTempTable("orders_df")
  val eresult = sqlCtx.sql("SELECT o.yyyymmdd as dt,  count(1) as cnt, sum(o.qty) as qty, sum(o.amt) as amt,productid as productid,p.name as productname FROM products_df p RIGHT OUTER JOIN orders_df o ON (p.id = o.productid) GROUP BY o.yyyymmdd, o.productid,p.name ORDER BY o.yyyymmdd, sum(o.qty) DESC").cache()
  val noofrec = eresult.count()
  eresult.write.mode(SaveMode.Overwrite).parquet("/user/oc/spark/orders_sum/")

  println("----------------------------------------")
  println("Order Summary Stats:-")
  println("----------------------------------------")
  println("Total No of Records : %s".format(noofrec))
  println("----------------------------------------")
  }
}


vi OrderSumPrqtDaily.sbt

name := "OrderSumPrqtDaily"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "1.5.0"



spark-submit --conf spark.root.logger=ERROR,console --class OrderSumPrqtDaily --master local[2]   /home/poc/OrderSumPrqtDaily/target/scala-2.10/ordersumprqtdaily_2.10-1.0.jar

spark-submit --conf spark.root.logger=ERROR,console --class OrderSumPrqtDaily /home/poc/oc/oozie/wf1/ordersumprqtdaily_2.10-1.0.jar \
             --master yarn --deploy-mode "cluster" --driver-memory 500m --num-executors 4 --executor-memory 1g --executor-cores 1 

------------------------------------------------------------------------------------------------
--STEP#6. Run Spark SQL on Hive External Tables and Save Summary Data
------------------------------------------------------------------------------------------------
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

val hiveCtx = new HiveContext(sc)
val eresult= hiveCtx.sql("select * from oc.product_prqt p RIGHT OUTER JOIN oc.order_prqt o ON ( p.id = o.productid) ").cache()
eresult.show(10000)

------------------------------------------------------------------------------------------------
--STEP#7. Create Hbase Table
------------------------------------------------------------------------------------------------


+----+-----------+-------+-----------+--------------------+--------+----------+---------+----------+---+-----+---------+--------+----+
|  id|       name|  price|   category|                date|      dt| timestamp|productid|customerid|qty|price|      amt|yyyymmdd|hour|
+----+-----------+-------+-----------+--------------------+--------+----------+---------+----------+---+-----+---------+--------+----+
|   5|Hadoop Book|   9.99|      Books|Wed Apr 20 01:59:...|20160420|1461131997|        5|         5|  7|15.00|105.99000|20160420|   2|
|   6|  Hive Book|   8.99|      Books|Wed Apr 20 01:59:...|20160420|1461131999|        6|         6|  8|16.00|128.99000|20160420|   2|
|   7| Spark Book|  19.99|      Books|Wed Apr 20 02:00:...|20160420|1461132001|        7|         7|  9|17.00|153.99000|20160420|   2|
+----+-----------+-------+-----------+--------------------+--------+----------+---------+----------+---+-----+---------+--------+----+
--
create_namespace 'oc'
--Create Hbase table with 3 ColumnFamily and Different Versions
create 'oc:orders', {NAME => 'o' ,VERSIONS => 1} ,{ NAME => 'p',VERSIONS => 2} , {NAME =>'c',VERSIONS => 3}

describe 'oc:orders'

--Record#1
put 'oc:orders', '20160420,5' , 'o:id',    '5'
put 'oc:orders', '20160420,5' , 'o:price', '9.99'
put 'oc:orders', '20160420,5' , 'o:qty',   '7'
put 'oc:orders', '20160420,5' , 'o:date',  'Wed Apr 20 01:59'

put 'oc:orders', '20160420,5' , 'p:id',    'p1'
put 'oc:orders', '20160420,5' , 'p:name',  'Hadoop Book'
put 'oc:orders', '20160420,5' , 'p:cat',   'Books'

put 'oc:orders', '20160420,5' , 'p:id',    'p2'
put 'oc:orders', '20160420,5' , 'p:name',  'Hive Book'
put 'oc:orders', '20160420,5' , 'p:cat',   'Books'

put 'oc:orders', '20160420,5' , 'c:id',    'c1'
put 'oc:orders', '20160420,5' , 'c:name',  'Abe'
put 'oc:orders', '20160420,5' , 'c:city',  'Bern'

--Record#2
put 'oc:orders', '20160420,6' , 'o:id',    '6'
put 'oc:orders', '20160420,6' , 'o:price', '9.99'
put 'oc:orders', '20160420,6' , 'o:qty',   '7'
put 'oc:orders', '20160420,6' , 'o:date',  'Wed Apr 21 01:59'

put 'oc:orders', '20160420,6' , 'p:id',    'p1'
put 'oc:orders', '20160420,6' , 'p:name',  'Hadoop Book'
put 'oc:orders', '20160420,6' , 'p:cat',   'Books'

put 'oc:orders', '20160420,6' , 'c:id',    'c2'
put 'oc:orders', '20160420,6' , 'c:name',  'Zac'
put 'oc:orders', '20160420,6' , 'c:city',  'Zurich'

--Record#3
put 'oc:orders', '20160420,7' , 'o:id',    '7'
put 'oc:orders', '20160420,7' , 'o:price', '19.99'
put 'oc:orders', '20160420,7' , 'o:qty',   '7'
put 'oc:orders', '20160420,7' , 'o:date',  'Wed Apr 22 01:59'

put 'oc:orders', '20160420,7' , 'p:id',    'p1'
put 'oc:orders', '20160420,7' , 'p:name',  'Spark Book'
put 'oc:orders', '20160420,7' , 'p:cat',   'Books'

put 'oc:orders', '20160420,7' , 'c:id',    'c3'
put 'oc:orders', '20160420,7' , 'c:name',  'Gen'
put 'oc:orders', '20160420,7' , 'c:city',  'Geneva'

--
deleteall 'oc:orders', '20160420,5'
--
deleteall 'oc:orders', '20160420,5', 'o'
delete 'oc:orders', '20160420,5', 'p'
delete 'oc:orders', '20160420,5', 'c'

--
get 'oc:orders', '20160420,5' 


get 'oc:orders', '20160420,5', {COLUMN => ['o', 'p', 'c'],VERSIONS => 2}











