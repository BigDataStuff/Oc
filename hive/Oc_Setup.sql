------------------------------------------------------------------------------------------------
--FLUME SETUP STEP#1. Create Flume Agent using hdfs sink RollSize 1000 record into 1 File.
------------------------------------------------------------------------------------------------
FlumeOrderDailyA1.sources = s1
FlumeOrderDailyA1.channels = c1
FlumeOrderDailyA1.sinks = k1

# Define Source
FlumeOrderDailyA1.sources.s1.type = exec
FlumeOrderDailyA1.sources.s1.command = tail -F /home/poc/order_daily.log
FlumeOrderDailyA1.sources.s1.restart = true
FlumeOrderDailyA1.sources.s1.batchSize = 1000

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
FlumeOrderDailyA1.sinks.k1.hdfs.rollCount = 10000
FlumeOrderDailyA1.sinks.k1.hdfs.batchSize = 1000
FlumeOrderDailyA1.sinks.k1.hdfs.idleTimeout = 10
FlumeOrderDailyA1.sinks.k1.hdfs.useLocalTimeStamp = true

# Use channel which buffers events in memory
FlumeOrderDailyA1.channels.c1.type = memory
FlumeOrderDailyA1.channels.c1.capacity=100000
FlumeOrderDailyA1.channels.c1.transactionCapacity=10000

FlumeOrderDailyA1.sources.s1.channels = c1
FlumeOrderDailyA1.sinks.k1.channel = c1

------------------------------------------------------------------------------------------------
--FLUME EXECUTE STEP#2. Start Flume Agent in Background
------------------------------------------------------------------------------------------------
nohup flume-ng agent --conf /etc/flume/conf/ --name FlumeOrderDailyA1 --conf-file FlumeOrderDailyA1.conf &

------------------------------------------------------------------------------------------------
--HIVE SETUP STEP#1. Create External table to Access Flume Log Data
------------------------------------------------------------------------------------------------
DROP TABLE order_ext_flume;
CREATE EXTERNAL TABLE order_ext_flume(
Date        String,
Dt          INT,
TimeStamp   BIGINT,
productid   INT,
customerid  INT,
qty         INT,
price       DECIMAL(10,2),
amt         DECIMAL(10,5)
)
PARTITIONED BY ( yyyymmdd int, hour int )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/oc/flume/logs/'
;

------------------------------------------------------------------------------------------------
--HIVE SETUP STEP#2. Create Managed Parquet table to stored Partitioned data by Date and Hour (24 hours * 3 Days)
------------------------------------------------------------------------------------------------
DROP TABLE order_prqt;
CREATE TABLE order_prqt(
Date        String,
Dt          INT,
TimeStamp   BIGINT,
productid   INT,
customerid  INT,
qty         INT,
price       DECIMAL(10,2),
amt         DECIMAL(10,5)
)
PARTITIONED BY ( yyyymmdd int, hour int )
STORED AS PARQUET
LOCATION '/user/oc/hive/mng/order_prqt'
TBLPROPERTIES ("parquet.compression"="SNAPPY")
;

------------------------------------------------------------------------------------------------
--HIVE SETUP STEP#3. Create Managed Parquet table to stored Partitioned data by Date (3 Files for each day Day)
------------------------------------------------------------------------------------------------
DROP TABLE order_prqt_daily;
CREATE  TABLE order_prqt_daily(
Date        String,
hour        Int,
Dt          INT,
TimeStamp   BIGINT,
productid   INT,
customerid  INT,
qty         INT,
price       DECIMAL(10,2),
amt         DECIMAL(10,5)
)
PARTITIONED BY ( yyyymmdd int)
STORED AS PARQUET
LOCATION '/user/oc/hive/mng/order_prqt_daily'
TBLPROPERTIES ("parquet.compression"="SNAPPY")
;

------------------------------------------------------------------------------------------------
--HIVE SETUP STEP#4. Create External table to Access Spark Summary Data by Product 
------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE order_sum_spark(
dt             INT,
cnt            BIGINT,
qty            BIGINT,
amt            DECIMAL(20,5),
product_id     INT,
productname    String
)
STORED AS PARQUET
LOCATION '/user/oc/spark/orders_sum/'
;

-------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------
--HIVE LOAD STEP#1. Add Partition to  External table for Flume Data.
------------------------------------------------------------------------------------------------
set hivevar:iyyyymmdd=20160520;
set hivevar:ihour=22;

vi addExtPartTo_order_ext_flume.hql

--Load External Staging Table for a Day
--Drop Partition if Already Exist
ALTER TABLE oc.order_ext_flume DROP PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  );
--Create new Partition
ALTER TABLE oc.order_ext_flume ADD  PARTITION(yyyymmdd=${iyyyymmdd}, hour=${ihour}  ) LOCATION '/user/oc/flume/logs/${iyyyymmdd}/${ihour}';

------------------------------------------------------------------------------------------------
--HIVE LOAD STEP#2. Load Managed(Daily_Hourly) Parquet Partition table using  External table with Compression
------------------------------------------------------------------------------------------------
vim addPartTo_order_prqt.hql
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
--HIVE LOAD STEP#3. Load Managed(Daily) Parquet Partition table using  Parquet Daily_Hourly
------------------------------------------------------------------------------------------------
vim addPartTo_order_prqt_daily.hql
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

set hive.exec.dynamic.partition.mode=nonstrict;
--Dynamic Partition
FROM oc.order_prqt stg
INSERT OVERWRITE TABLE oc.order_prqt_daily PARTITION(yyyymmdd )
SELECT 
Date,  
hour,   
Dt,         
TimeStamp,  
productid, 
customerid, 
qty,        
price,      
amt,
yyyymmdd    
WHERE 1=1
   AND stg.yyyymmdd=${iyyyymmdd}
;

--Gather Stats
ANALYZE TABLE oc.order_prqt_daily       PARTITION(yyyymmdd=${iyyyymmdd}  )   COMPUTE STATISTICS;

------------------------------------------------------------------------------------------------
--SPARK EXECUTE STEP#1. Access Parquet File Using Dataframe and Summarize Data at Date, Product Level
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
--KAFKA SETUP STEP#1. Start the Kafka Server as Root if its Not Started
------------------------------------------------------------------------------------------------
/home/kafka/kafka/kafka/bin/kafka-server-start.sh /home/kafka/kafka/kafka/config/server.properties

cp /home/kafka/kafka/kafka/config/server.properties /home/kafka/kafka/kafka/config/server-1.properties
cp /home/kafka/kafka/kafka/config/server.properties /home/kafka/kafka/kafka/config/server-2.properties

--Setting up a multi-broker cluster
/home/kafka/kafka/kafka/bin/kafka-server-start.sh /home/kafka/kafka/kafka/config/server.properties   &
/home/kafka/kafka/kafka/bin/kafka-server-start.sh /home/kafka/kafka/kafka/config/server-1.properties &
/home/kafka/kafka/kafka/bin/kafka-server-start.sh /home/kafka/kafka/kafka/config/server-2.properties &

------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#2. Create a new Topic ; If existing then check using --List
------------------------------------------------------------------------------------------------
/home/kafka/kafka/kafka/bin/kafka-topics.sh --create --zookeeper nn1.app.com:2181 --replication-factor 1 --partitions 1 --topic test

--We can now see that topic if we run the list topic command:
/home/kafka/kafka/kafka/bin/kafka-topics.sh --list --zookeeper nn1.app.com:2181


/home/kafka/kafka/kafka/bin/kafka-topics.sh --create --zookeeper nn1.app.com:2181 --replication-factor 3 --partitions 4 --topic order
--To check with broker doing what
/home/kafka/kafka/kafka/bin/kafka-topics.sh --describe --zookeeper nn1.app.com:2181 --topic order

------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#3. To Consume 
------------------------------------------------------------------------------------------------
/home/kafka/kafka/kafka/bin/kafka-console-consumer.sh --zookeeper nn1.app.com:2181 --topic order --from-beginning

------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#4. Flume to Kafka Sink Setup
------------------------------------------------------------------------------------------------
vi FlumeToKafkaSinkA1.conf
FlumeToKafkaSinkA1.sources = s1
FlumeToKafkaSinkA1.channels = c1
FlumeToKafkaSinkA1.sinks = k1

# Define Source
FlumeToKafkaSinkA1.sources.s1.type = exec
FlumeToKafkaSinkA1.sources.s1.command = tail -F /home/poc/order.log
FlumeToKafkaSinkA1.sources.s1.restart = true
FlumeToKafkaSinkA1.sources.s1.batchSize = 1

# Kafka sinks
FlumeToKafkaSinkA1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
FlumeToKafkaSinkA1.sinks.k1.topic = order
FlumeToKafkaSinkA1.sinks.k1.brokerList = nn1.app.com:9092,nn1.app.com:9093,nn1.app.com:9094
FlumeToKafkaSinkA1.sinks.k1.requiredAcks = 1
FlumeToKafkaSinkA1.sinks.k1.batchSize = 1

# Use channel which buffers events in memory
FlumeToKafkaSinkA1.channels.c1.type = memory
FlumeToKafkaSinkA1.channels.c1.capacity=1000

FlumeToKafkaSinkA1.sources.s1.channels = c1
FlumeToKafkaSinkA1.sinks.k1.channel = c1

------------------------------------------------------------------------------------------------
vi FlumeToKafkaSinkA2.conf

FlumeToKafkaSinkA2.sources = s1
FlumeToKafkaSinkA2.channels = c1
FlumeToKafkaSinkA2.sinks = k1

# Define Source
FlumeToKafkaSinkA2.sources.s1.type = exec
FlumeToKafkaSinkA2.sources.s1.command = tail -F /home/poc/order2.log
FlumeToKafkaSinkA2.sources.s1.restart = true
FlumeToKafkaSinkA2.sources.s1.batchSize = 10

# Kafka sinks
FlumeToKafkaSinkA2.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
FlumeToKafkaSinkA2.sinks.k1.topic = order
FlumeToKafkaSinkA2.sinks.k1.brokerList = nn1.app.com:9092,nn1.app.com:9093,nn1.app.com:9094
FlumeToKafkaSinkA2.sinks.k1.requiredAcks = 1
FlumeToKafkaSinkA2.sinks.k1.batchSize = 10

# Use channel which buffers events in memory
FlumeToKafkaSinkA2.channels.c1.type = memory
FlumeToKafkaSinkA2.channels.c1.capacity=1000

FlumeToKafkaSinkA2.sources.s1.channels = c1
FlumeToKafkaSinkA2.sinks.k1.channel = c1


------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#5. Starting Flume to Kafka Sink Agents
------------------------------------------------------------------------------------------------
flume-ng agent --conf /etc/flume/conf/ --name FlumeToKafkaSinkA1 --conf-file FlumeToKafkaSinkA1.conf
flume-ng agent --conf /etc/flume/conf/ --name FlumeToKafkaSinkA2 --conf-file FlumeToKafkaSinkA2.conf


------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#6. Start SparkStreaming proceessing USING KafkaSink using Two Receiver
------------------------------------------------------------------------------------------------
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

val topicMap = Map("order"->1)
val streamingContext = new StreamingContext(sc, Seconds(5))
val lines = KafkaUtils.createStream(streamingContext, "nn1.app.com:2181", "groupid", topicMap).map(_._2)
val nooforder= lines.count()
nooforder.print()
val order = lines.map(x=>x.split('|')).map(x =>(x(0),x(1).toInt,x(2).toLong,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toDouble,x(7).toDouble,x(8).toInt )  )
order.saveAsTextFiles("/user/oc/sparkstream/order/",".txt")

val order = lines.map(x=>x.split('|')).map( x =>(x(3).toInt,x(7).toDouble ) )
val obyp  = order.reduceByKey((x,y) => x+y)
obyp.print()

streamingContext.start()  
streamingContext.awaitTermination()   



------------------------------------------------------------------------------------------------
--KAFKA SETUP STEP#6. Start SparkStreaming proceessing USING KafkaSink using Two Receiver by joining product
--Using DataFrame
------------------------------------------------------------------------------------------------
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SQLContext
import sqlContext.implicits._
import org.apache.spark.rdd.RDD

/** Case class for converting RDD to DataFrame */
case class order_cls(date: String, dt: Int,timestamp: Long,productid: Int,customerid: Int,qty: Int,price: Double,amt: Double )

// Create the context with a 2 second batch size
val topicMap = Map("order"->1)
val streamingContext = new StreamingContext(sc, Seconds(5))

// Create DStream
val lines = KafkaUtils.createStream(streamingContext, "nn1.app.com:2181", "groupid", topicMap).map(_._2)
val nooforder= lines.count()


lines.foreachRDD{(rdd: RDD[String], time: Time) =>
val sqlCtx = SQLContext.getOrCreate(rdd.sparkContext)
import sqlContext.implicits._
val order = rdd.map(x=>x.split('|')).map( x =>order_cls(x(0),x(1).toInt,x(2).toLong,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toDouble,x(7).toDouble )).toDF()
val pfile = sqlCtx.read.format("parquet").load("/user/oc/hive/mng/oc/product_prqt/")
val eresult = order.join(pfile,  order("productid") === pfile("id")).groupBy(order("dt"),order("productid"),pfile("name")).agg(count(order("productid")) as "cnt",sum(order("qty")) as "qty", sum(order("amt")) as "amt")
eresult.show()
}













