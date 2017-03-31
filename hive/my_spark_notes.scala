

Creating Classes in Scala/Spark

class emp(id: Int,nm: String,dId: Int) extends java.io.Serializable{
  val empId:  Int    =id
  val empNm:  String =nm
  val salary: Double =0
  val deptId: Int    =dId

  def getSalary():Double = {
      val salary = ( id * 1000) + .75
      return salary
  }
}

class dept(id: Int, nm: String) extends java.io.Serializable {
  val deptId: Int = id 
  val deptNm: String = nm 
}

val e1=new emp(1,"John",10); 
val e2=new emp(2,"Sal",20); 
val e3=new emp(3,"Sally",20); 
val e4=new emp(4,"Joe",40); 
val e5=new emp(5,"Tom",40);
val e6=new emp(6,"Zac",60);

val d1=new dept(10,"Finance"); 
val d2=new dept(20,"Sales"); 
val d3=new dept(30,"Hr"); 
val d4=new dept(40,"IT")

Transformation on One RDD

val emp     = sc.parallelize(List(e1,e2,e3,e2,e4,e5,e6))
val ekv     = emp.map(x=> (x.empId, x.getSalary))
------------------------------------------------
(1,1000.75)
(2,2000.75)
(3,3000.75)
(2,2000.75)
(4,4000.75)
(5,5000.75)
(6,6000.75)
------------------------------------------------
Custom Partition in Reduce By

scala> ekv.reduceByKey((x,y) => x + y,4).sortByKey().collect.foreach(println)
(1,1000.75)
(2,4001.5)
(3,3000.75)
(4,4000.75)
(5,5000.75)
(6,6000.75)

scala> ekv.groupByKey().sortByKey().collect.foreach(println)
(1,CompactBuffer(1000.75))
(2,CompactBuffer(2000.75, 2000.75))
(3,CompactBuffer(3000.75))
(4,CompactBuffer(4000.75))
(5,CompactBuffer(5000.75))
(6,CompactBuffer(6000.75))

scala> ekv.countByValue().foreach(println)
((5,5000.75),1)
((4,4000.75),1)
((6,6000.75),1)
((1,1000.75),1)
((2,2000.75),2)
((3,3000.75),1)

scala> ekv.countByKey().foreach(println)
(5,1)
(1,1)
(6,1)
(2,2)
(3,1)
(4,1)

scala> ekv.mapValues(x=> x+ 100).sortByKey().collect.foreach(println)
(1,1100.75)
(2,2100.75)
(2,2100.75)
(3,3100.75)
(4,4100.75)
(5,5100.75)
(6,6100.75)

scala> ekv.keys.collect.foreach(println)
1
2
3
2
4
5
6

scala> ekv.values.collect.foreach(println)
1000.75
2000.75
3000.75
2000.75
4000.75
5000.75
6000.75

scala> ekv.filter{ case(key,value) => value > 3000 }.collect.foreach(println)
(3,3000.75)
(4,4000.75)
(5,5000.75)
(6,6000.75)

scala> ekv.collectAsMap()
res191: scala.collection.Map[Int,Double] = Map(2 -> 2000.75, 5 -> 5000.75, 4 -> 4000.75, 1 -> 1000.75, 3 -> 3000.75, 6 -> 6000.75)

scala> ekv.lookup(2)
res194: Seq[Double] = WrappedArray(2000.75, 2000.75)


Transformation on Two RDDs

val emp     = sc.parallelize(List(e1,e2,e3,e2,e4,e5,e6)).cache()
val dept    = sc.parallelize(List(d1,d2,d3,d4)).cache()

val ekv     =  emp.map(x=> (x.deptId,x.empId + " "+ x.empNm))
val dkv     = dept.map(x=> (x.deptId,x.deptNm))
-----------------------------------------
scala> ekv.collect.foreach(println)
(10,1 John)
(20,2 Sal)
(20,3 Sally)
(20,2 Sal)
(40,4 Joe)
(40,5 Tom)
(60,6 Zac)

scala> dkv.collect.foreach(println)
(10,Finance)
(20,Sales)
(30,Hr)
(40,IT)
-----------------------------------------

scala> emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).join(dept.map(x=> (x.deptId,x.deptNm) )).sortByKey().collect.foreach(println)
(10,(1 John,Finance))
(20,(2 Sal,Sales))
(20,(3 Sally,Sales))
(20,(2 Sal,Sales))
(40,(4 Joe,IT))
(40,(5 Tom,IT))

scala> emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).leftOuterJoin(dept.map(x=> (x.deptId,x.deptNm) )).sortByKey().collect.foreach(println)
(10,(1 John,Some(Finance)))
(20,(2 Sal,Some(Sales)))
(20,(3 Sally,Some(Sales)))
(20,(2 Sal,Some(Sales)))
(40,(4 Joe,Some(IT)))
(40,(5 Tom,Some(IT)))
(60,(6 Zac,None))

scala> emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).rightOuterJoin(dept.map(x=> (x.deptId,x.deptNm) )).sortByKey().collect.foreach(println)
(10,(Some(1 John),Finance))
(20,(Some(2 Sal),Sales))
(20,(Some(3 Sally),Sales))
(20,(Some(2 Sal),Sales))
(30,(None,Hr))
(40,(Some(4 Joe),IT))
(40,(Some(5 Tom),IT))

scala> emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).cogroup(dept.map(x=> (x.deptId,x.deptNm) )).sortByKey().collect.foreach(println)
(10,(CompactBuffer(1 John),CompactBuffer(Finance)))
(20,(CompactBuffer(2 Sal, 3 Sally, 2 Sal),CompactBuffer(Sales)))
(30,(CompactBuffer(),CompactBuffer(Hr)))
(40,(CompactBuffer(4 Joe, 5 Tom),CompactBuffer(IT)))
(60,(CompactBuffer(6 Zac),CompactBuffer()))

scala>emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).subtractByKey(dept.map(x=> (x.deptId,x.deptNm) )).sortByKey().collect.foreach(println)
(60,6 Zac)


scala> val joined = emp.map(x=> (x.deptId,x.empId + " "+ x.empNm)).join(dept.map(x=> (x.deptId,x.deptNm) ))
scala> val fltr = joined.filter{case(deptid,(empid,deptnm)) => deptnm.contains("IT")}
scala> fltr.collect.foreach(println)
(40,(4 Joe,IT))
(40,(5 Tom,IT))

Word Count By another way
val f = sc.textFile("/user/poc/txt/word.txt").flatMap(x=> x.split(" ")).countByValue().foreach(println)

Reading WholeText File and Calculating No of Char in each File
val input = sc.wholeTextFiles("/user/poc/txt/*.txt")
val input = sc.wholeTextFiles("/user/hive/warehouse/")
input.mapValues( x=> x.split(" ").map(y=>y.length).reduce((x,y)=> x+y) ).collect.foreach(println)

(hdfs://quickstart.cloudera:8020/user/poc/txt/country.txt,78)                   
(hdfs://quickstart.cloudera:8020/user/poc/txt/f1.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f2.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f3.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f4.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f5.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f7.txt,50660214)
(hdfs://quickstart.cloudera:8020/user/poc/txt/f8.txt,75990321)
(hdfs://quickstart.cloudera:8020/user/poc/txt/filelist.txt,25330107)
(hdfs://quickstart.cloudera:8020/user/poc/txt/sales.txt,80)
(hdfs://quickstart.cloudera:8020/user/poc/txt/small.txt,32)
(hdfs://quickstart.cloudera:8020/user/poc/txt/word.txt,116)


Spark SQL is an extension of RDD called DataFrame(Schema RDD). Dataframe contains an RDD of Row Objects each represeting record.
Two Entry Points
SQLContext   ==>Basic SQL without Hive Dependency
HiveContext  ==>To connect Spark SQL to existing Hive installation you must copy your hive-site.xml to Spark Conf Dir ($SPARK_HOME/conf)
Sample JSON data files
emp.json (Columns/Array/Map/Struct )
{"id":1,"name":"Joe","salary":10000,"mgr":["Smith","Todd"],"tax":{"State":0.5,"Federal":0.2,"Insurance":0.1},"address":{"street":"1 Michigan Ave","city":"Chicago","state":"IL","zip":60600}}
{"id":2,"name":"Jay","salary":10000,"mgr":["Smith","Todd"],"tax":{"State":0.5,"Federal":0.2,"Insurance":0.1},"address":{"city":"Chicago","state":"IL","zip":60600}}
{"id":3,"name":"John","salary":10000,                      "tax":{"State":0.5,"Federal":0.2},                "address":{"street":"Park Ave","city":"New York","state":"NY","zip":10020}}
{"id":4,"name":"Zac","salary":10000,"mgr":["Smith","John"],"tax":{"State":0.5,"Federal":0.2,"Insurance":0.1},"address":{"street":"1 Michigan Ave","city":"Chicago","state":"IL","zip":60600}}
{"id":5,"name":"Sam","salary":10000,"mgr":["Smith","John"],"tax":{"Federal":0.2,"Insurance":0.1},            "address":{"street":"1 Mic Ave","city":"New York","state":"NY","zip":10010}}
{"id":6,"name":"Ray",               "mgr":["John"],        "tax":{"State":0.5,"Insurance":0.1},              "address":{"street":"100 Newark Ave","city":"Jersey City","state":"NJ","zip":7302}}
{"id":7,"name":"Duc","salary":10000,                                                                         "address":{"street":"Some Street in USA"}}

students.json (Columns/Array/Struct )
{"id":1, "name":"Sam",    "cities":["Palo Alto", "Menlo Park"], "schools":[{"sname":"Stanford", "year":2010}, {"sname":"Berkeley", "year":2012}]}
{"id":2, "name":"Zac",    "cities":["Santa Cruz"],              "schools":[{"sname":"UCSB",     "year":2011}]}
{"id":3, "name":"Mac",    "cities":["Portland"],                "schools":[{"sname":"Berkeley", "year":2014}]}
{"id":4, "name":"Joe",    "cities":["Seattle","San Jose"]}
{"id":5, "name":"John","DoB":"01-Jan-2000"}

Creating DataFrames
import org.apache.spark.sql.SQLContext

val sqlCtx = new SQLContext(sc)

scala> val edf= sqlCtx.read.format("json").load("/user/poc/txt/emp.json")
edf: org.apache.spark.sql.DataFrame = [address: struct<city:string,state:string,street:string,zip:bigint>, id: bigint, mgr: array<string>, name: string, salary: bigint, tax: struct<Federal:double,Insurance:double,State:double>]

scala> val sdf= sqlCtx.read.format("json").load("/user/poc/txt/students.json")
sdf: org.apache.spark.sql.DataFrame = [DoB: string, cities: array<string>, id: bigint, name: string, schools: array<struct<sname:string,year:bigint>>]
DataFrame Operations
scala> edf.show()
+--------------------+---+-------------+----+------+--------------+
|             address| id|          mgr|name|salary|           tax|
+--------------------+---+-------------+----+------+--------------+
|[Chicago,IL,1 Mic...|  1|[Smith, Todd]| Joe| 10000| [0.2,0.1,0.5]|
|[Chicago,IL,null,...|  2|[Smith, Todd]| Jay| 10000| [0.2,0.1,0.5]|
|[New York,NY,Park...|  3|         null|John| 10000|[0.2,null,0.5]|
|[Chicago,IL,1 Mic...|  4|[Smith, John]| Zac| 10000| [0.2,0.1,0.5]|
|[New York,NY,1 Mi...|  5|[Smith, John]| Sam| 10000|[0.2,0.1,null]|
|[Jersey City,NJ,1...|  6|       [John]| Ray|  null|[null,0.1,0.5]|
|[null,null,Some S...|  7|         null| Duc| 10000|          null|
+--------------------+---+-------------+----+------+--------------+

scala> edf.printSchema()
root
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- street: string (nullable = true)
 |    |-- zip: long (nullable = true)
 |-- id: long (nullable = true)
 |-- mgr: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- name: string (nullable = true)
 |-- salary: long (nullable = true)
 |-- tax: struct (nullable = true)
 |    |-- Federal: double (nullable = true)
 |    |-- Insurance: double (nullable = true)
 |    |-- State: double (nullable = true)

scala> edf.select("name").show
+----+
|name|
+----+
| Joe|
| Jay|
|John|
| Zac|
| Sam|
| Ray|
| Duc|

scala> edf.select(edf("id"), edf("name"), edf("salary"), edf("tax"), edf("mgr") ).show
+---+----+------+--------------+-------------+
| id|name|salary|           tax|          mgr|
+---+----+------+--------------+-------------+
|  1| Joe| 10000| [0.2,0.1,0.5]|[Smith, Todd]|
|  2| Jay| 10000| [0.2,0.1,0.5]|[Smith, Todd]|
|  3|John| 10000|[0.2,null,0.5]|         null|
|  4| Zac| 10000| [0.2,0.1,0.5]|[Smith, John]|
|  5| Sam| 10000|[0.2,0.1,null]|[Smith, John]|
|  6| Ray|  null|[null,0.1,0.5]|       [John]|
|  7| Duc| 10000|          null|         null|
+---+----+------+--------------+-------------+

scala> edf.filter(edf("id") > 4).show
+--------------------+---+-------------+----+------+--------------+
|             address| id|          mgr|name|salary|           tax|
+--------------------+---+-------------+----+------+--------------+
|[New York,NY,1 Mi...|  5|[Smith, John]| Sam| 10000|[0.2,0.1,null]|
|[Jersey City,NJ,1...|  6|       [John]| Ray|  null|[null,0.1,0.5]|
|[null,null,Some S...|  7|         null| Duc| 10000|          null|
+--------------------+---+-------------+----+------+--------------+

scala> edf.groupBy("mgr").count().show
+-------------+-----+                                                           
|          mgr|count|
+-------------+-----+
|[Smith, John]|    2|
|[Smith, Todd]|    2|
|         null|    2|
|       [John]|    1|
+-------------+-----+

scala> val list = edf.select(edf("id"), edf("name"), edf("mgr"))
list: org.apache.spark.sql.DataFrame = [id: bigint, name: string, mgr: array<string>]

scala> list.collect.foreach(println)
[1,Joe,WrappedArray(Smith, Todd)]
[2,Jay,WrappedArray(Smith, Todd)]
[3,John,null]
[4,Zac,WrappedArray(Smith, John)]
[5,Sam,WrappedArray(Smith, John)]
[6,Ray,WrappedArray(John)]
[7,Duc,null]

Register DataFrames as temporary Tables
import org.apache.spark.sql.SQLContext

val sqlCtx = new SQLContext(sc)

scala> val edf= sqlCtx.read.format("json").load("/user/poc/txt/emp.json")
edf: org.apache.spark.sql.DataFrame = [address: struct<city:string,state:string,street:string,zip:bigint>, id: bigint, mgr: array<string>, name: string, salary: bigint, tax: struct<Federal:double,Insurance:double,State:double>]

scala> val sdf= sqlCtx.read.format("json").load("/user/poc/txt/students.json")
sdf: org.apache.spark.sql.DataFrame = [DoB: string, cities: array<string>, id: bigint, name: string, schools: array<struct<sname:string,year:bigint>>]

scala> edf.registerTempTable("emp")
scala> edf.registerTempTable("students")

scala> val eresult= sqlCtx.sql("Select * from emp")
eresult: org.apache.spark.sql.DataFrame = [address: struct<city:string,state:string,street:string,zip:bigint>, id: bigint, mgr: array<string>, name: string, salary: bigint, tax: struct<Federal:double,Insurance:double,State:double>]

scala> eresult.collect
res98: Array[org.apache.spark.sql.Row] = Array([[Chicago,IL,1 Michigan Ave,60600],1,WrappedArray(Smith, Todd),Joe,10000,[0.2,0.1,0.5]], [[Chicago,IL,null,60600],2,WrappedArray(Smith, Todd),Jay,10000,[0.2,0.1,0.5]], [[New York,NY,Park Ave,10020],3,null,John,10000,[0.2,null,0.5]], [[Chicago,IL,1 Michigan Ave,60600],4,WrappedArray(Smith, John),Zac,10000,[0.2,0.1,0.5]], [[New York,NY,1 Mic Ave,10010],5,WrappedArray(Smith, John),Sam,10000,[0.2,0.1,null]], [[Jersey City,NJ,100 Newark Ave,7302],6,WrappedArray(John),Ray,null,[null,0.1,0.5]], [[null,null,Some Street in USA,null],7,null,Duc,10000,null])

scala> eresult.collect.foreach(println)
[[Chicago,IL,1 Michigan Ave,60600],1,WrappedArray(Smith, Todd),Joe,10000,[0.2,0.1,0.5]]
[[Chicago,IL,null,60600],2,WrappedArray(Smith, Todd),Jay,10000,[0.2,0.1,0.5]]
[[New York,NY,Park Ave,10020],3,null,John,10000,[0.2,null,0.5]]
[[Chicago,IL,1 Michigan Ave,60600],4,WrappedArray(Smith, John),Zac,10000,[0.2,0.1,0.5]]
[[New York,NY,1 Mic Ave,10010],5,WrappedArray(Smith, John),Sam,10000,[0.2,0.1,null]]
[[Jersey City,NJ,100 Newark Ave,7302],6,WrappedArray(John),Ray,null,[null,0.1,0.5]]
[[null,null,Some Street in USA,null],7,null,Duc,10000,null]

scala> val eresult = sqlCtx.sql("Select id,name,address FROM emp WHERE tax IS NULL")
eresult: org.apache.spark.sql.DataFrame = [id: bigint, name: string, address: struct<city:string,state:string,street:string,zip:bigint>]

scala> eresult.collect
res109: Array[org.apache.spark.sql.Row] = Array([7,Duc,[null,null,Some Street in USA,null]])

scala> eresult.collect.foreach(println)
[7,Duc,[null,null,Some Street in USA,null]]

Converting between DataFrames and RDDs
import org.apache.spark.sql.hive.HiveContext

val hiveCtx = new HiveContext(sc)
val edf= hiveCtx.read.format("json").load("/user/poc/txt/emp.json")
edf.registerTempTable("emp")
val eresult= hiveCtx.sql("Select id,name from emp")
scala> val ename = eresult.rdd.map(row=> ("Hi," + row.getString(1) ))
ename: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[555] at map at <console>:35
scala> ename.foreach(println)
Hi,Joe
Hi,Jay
Hi,John
Hi,Zac
Hi,Sam
Hi,Ray
Hi,Duc


You can query for the values of Spark properties in Spark shell as follows:
scala> sc.getConf.getOption("spark.local.dir")
res0: Option[String] = None

scala> sc.getConf.getOption("spark.app.name")
res1: Option[String] = Some(Spark shell)

scala> sc.getConf.get("spark.master")
res2: String = yarn-client
scala> sc.sparkUser
res11: String = poc
sc.getConf.get("spark.executor.id")
sc.getConf.get("spark.app.id")


Setting up Properties
There are the following ways to set up properties for Spark and user programs (in the order of importance from the least important to the most important):
conf/spark-defaults.conf - the default
--conf - the command line option used by spark-shell and spark-submit
SparkConf
Default Configuration
The default Spark configuration is created when you execute the following code:
import org.apache.spark.SparkConf
val conf = new SparkConf
It merely loads any spark.* system properties.
You can use conf.toDebugString or conf.getAll to have the spark.* system properties loaded printed out.
scala> conf.getAll
res0: Array[(String, String)] = Array((spark.app.name,Spark shell), (spark.jars,""), (spark.master,local[*]), (spark.submit.deployMode,client))

scala> conf.toDebugString
res1: String =
spark.app.name=Spark shell
spark.jars=
spark.master=local[*]
spark.submit.deployMode=client

scala> println(conf.toDebugString)
spark.app.name=Spark shell
spark.jars=
spark.master=local[*]
spark.submit.deployMode=client
Creating RDD
SparkContext allows you to create many different RDDs from input sources like:
Scala’s collections, i.e. sc.parallelize(0 to 100)
local or remote filesystems, i.e. sc.textFile("README.md")
Any Hadoop InputSource using sc.newAPIHadoopFile




Understand closures (what not to do)
Don't do this
// this runs in the driver
val foo = new SomeExpensiveNotSerializableThing
someRdd.map { x =>
  // this runs in the executor, so...
  // the attempt to close over foo will throw NotSerializableException
  foo.mangle(x)
}
Don't do this either
someRdd.map { x =>
  // this runs in the executor, ok...
  // but gets constructed for every element in the RDD, so will be slow
  val foo = new SomeExpensiveNotSerializableThing
  foo.mangle(x)
}

Understand closures (what to do)
Do this instead:
someRdd.mapPartitions { part =>
  // this runs in the executor, constructed only once per group of elements
  val foo = new SomeExpensiveNotSerializableThing
  part.map { x =>
    // then used for each element
    foo.mangle(x)
  }
}


Understand closures (other issues)
If you encounter other closure-related issues, make sure that
all captured references are serializable
all captured references are immutable
all captured references are either local vals, or in a static object
As a specific example, don't extend App:
object MyApp extends App {
  // start running
Use an explicit main method instead
object MyApp {
  def main(args: Array[String]) {
    // start running

Understand partitions
If data is partitioned into fewer tasks than there are executor cores, some cores may be idle
If there are many more tasks than cores, scheduler delay may be larger than work (delay is visible in the per-stage details link)
General rule of thumb, at least as many tasks as 2 * total number of executor cores
Set spark.default.parallelism, pass # of partitions to methods that accept it
Increase # partitions with rdd.repartition (which triggers a shuffle, so is costly), decrease with rdd.coalesce
If partitions are uneven in size, one executor may get overloaded
Common cause of unevenness is doing a grouping operation on an RDD where many keys are None; in this case, filter them out first
As always, MEASURE. There may be good reasons for a small or large # of partitions.
Understand shuffle
Whenever data needs to be redistributed among executors, a shuffle occurs
This typically occurs for by-key operations on PairRDD: groupBy, reduceByKey, etc.
Shuffle involves writing data to disk, then reading off of other executors' disks over the network.
Memory hierarchy 101: disk and network are slower than cache and ram, so shuffle is slow
Speeding things up requires either writing less data, or triggering fewer shuffles

Write less data
set spark.serializer = org.apache.spark.serializer.KryoSerializer
If you're using groupBy in order to do an aggregation you're probably doing it wrong.
reduceByKey, combineByKey, aggregateByKey do work before the shuffle, so they potentially write less data
Similar to combiners in hadoop

 In Scala : - It is an error to declare a value or variable without initializing it.



As an example, consider summing up the integer values of a list of chars. 
The initial value for the sum is 0. 
First, seqop transforms each input character to an Int and adds it to the sum (of the partition). 
Then, combop just needs to sum up the intermediate results of the partitions:
List('a', 'b', 'c').aggregate(0)({ (sum, ch) => sum + ch.toInt }, { (p1, p2) => p1 + p2 })


List(1,2,3,4,5).aggregate(0)( {(sum,i)=> sum + i}, {(p1,p2) => p1 + p2})
res73: Int = 15


Spark Streaming
Provides abstraction called Dstreams/discretized streams.
Dstreams is represented as sequence of RDDs.
Create StreamingContext with Batch Interval
For each input source, Spark streaming launches Receivers, which are tasks running within appplications executors.
Typically you might setup checkpointing every 5-10 batches of data.
In Stateless transformations each batch does not depend on data of its previous batches.
In stateful transformation, use data or intermediate results from previous batches based on sliding windows.
Transform operates directly on RDD inside Dstreams
Stateful Transformation has two types – windowed operation (sliding window) and updateStateByKey() to track the state across events for each key; - it requires checkpointing.
Windowed operations need two parameters – window duration and sliding duration both must be multiple of streaming contexts batch interval.
reduceByWindow() and reduceByKeyandWindow()
countByWindow() and countByValueandWindow()
updateStateByKey
foreachRDD() -similar to transform. Used for writing data to external database.
Kafka Source -   kafka.Utils.createStream(ssc, zkQuorum, group, topics)  Or kafkaUtils.createDirectStream[ string, string](ssc,kafkaParam,topicSet)
StreamingContext.getOrCreate(checkpointDir, CreateStreamingContext _ )

Spark SQL
DataFrame abstraction / Read structred format JSON,Parquet / Hive Tables / Database connectors(JDBC/ODBC)
DataFrame contains an RDD of Row Objects each representing record.
SQLContext or HiveContext
import org.apache.spark.sql.{DataFrame,Row}
Both loading data and executing queries return DataFrames. Under the hood, a DataFrame contains an RDD composed of Row Objects with additional schema inforamtion such as types in each column.
Use hiveCtx.cacheTable(“tablename”) 


YARN Resource Allocation Log






