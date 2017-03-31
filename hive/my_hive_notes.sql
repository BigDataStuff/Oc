a

Setting command line variable in Hive

hive --hiveconf hive.cli.print.current.db=true --hivevar dt=03282016 --hivevar region="USA" --hivevar db=dev

system:user.name=poc
hive> set dt;
dt=03282016
hive> set region;
region=USA
hive> set db;
db=dev
hive> set db=prod;
hive> set db;
db=prod
hive> set hivevar:region=EUROPE;
hive> set region;
region=EUROPE
hive (default)> set system:user.name=poc;
hive (default)> set system:user.name;
Using Varibles in Hive Command
hive> create table region( ${region} string);
hive> desc region;
europe              	string   

Executing the SQL cmd by passing variable in hive in non-interactive silent mode

[root@quickstart ~]# PROD_ID=1340; hive -S -e "SELECT product_id, product_name FROM products WHERE product_id= ${PROD_ID}";
1340	Majestic Men's Replica Texas Rangers Russell 

To search setting of particular variables

[root@quickstart ~]# hive -S -e "set" | grep warehouse
hive.metastore.warehouse.dir=/user/hive/warehouse
hive.warehouse.subdir.inherit.perms=true

Executing the SQL file by passing variable in hive in non-interactive silent mode
status.hql
--This is comments in hsql
SELECT count(*) as cnt FROM orders WHERE order_status='${i_order_status}';

hive -S -f status.hql --hivevar i_order_status=CLOSED;
7556
Executing the SQL file by passing variable in hive in Interactive mode
hive> SET hivevar:i_order_status=COMPLETE;
hive> source status.hql

The .hiverc File
ADD JAR /path/to/custom_hive_extensions.jar;
set hive.cli.print.current.db=true;
set hive.exec.mode.local.auto=true;
set hive.cli.print.header=true;

Creating Dummy (DUAL) table
CREATE TABLE src(s STRING);
echo "one row" > /tmp/myfile
hive -e "LOAD DATA LOCAL INPATH '/tmp/myfile' INTO TABLE src;

hive> select s, upper(string("1""1s")), length(s), substring(s,instr(s,' ')) from src;
	 one row	11S					7	 		row

Shell Execution
hive> ! pwd;
/home/cloudera

Hadoop dfs Commands from Inside Hive

hive> dfs -ls -R /user/hive/warehouse/products;
-rw-r--r--   1 cloudera supergroup          0 2016-03-26 20:58 /user/hive/warehouse/products/_SUCCESS
-rw-r--r--   1 cloudera supergroup     173315 2016-03-26 20:58 /user/hive/warehouse/products/part-m-00000

hive> dfs -cat /user/hive/warehouse/products/part-m-00000;

TIMESTAMPS are interpreted as UTC times
to_utc_timestamp and from_utc_timestamp
string format convention YYYY-MM-DD hh:mm:ss.fffffffff 
 To Interpret a string column as a number : - cast(s AS INT)

Collection Data Types

STRUCT  Like C struct or an object accessed using “dot.”/ "address":{"street":"Park Ave","city":"Chicago","state":"IL","zip":60600}
MAP     Key Value / "tax":{"State":0.5,"Federal":0.2,"Insurance":0.1}
ARRAY   Order seq of SameType using 0 based index/ "mgr":["Smith","Todd"]

 You’ll note that maps & structs are effectively the same thing in JSON.

DDL's

DROP TABLE emp; 
CREATE TABLE emp (
id              INT,
name            STRING,
salary          FLOAT,
mgr             ARRAY<STRING>,
tax             MAP<STRING, FLOAT>,
address         STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
ROW FORMAT       DELIMITED
FIELDS           TERMINATED BY ','  --'\001'
COLLECTION ITEMS TERMINATED BY ','  --'\002'
MAP KEYS         TERMINATED BY ':'  --'\003'
LINES            TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/mydb';

hive> dfs -ls -R /user/poc/txt/emp.json;
-rw-r--r--   1 cloudera supergroup       1260 2016-03-27 13:35 /user/poc/txt/emp.json

hive> dfs -ls -R /user/hive/warehouse/mydb;

hive> LOAD DATA INPATH '/user/poc/txt/emp.json' INTO TABLE emp;
Loading data to table default.emp
Table default.emp stats: [numFiles=1, totalSize=1260]

hive> dfs -ls -R /user/poc/txt/emp.json;
ls: `/user/poc/txt/emp.json': No such file or directory

hive> dfs -ls -R /user/hive/warehouse/mydb;
-rwxrwxrwx   1 cloudera supergroup       1260 2016-03-27 13:35 /user/hive/warehouse/mydb/emp.json

hive> select id,name,mgr,address.city from emp;

NULL	"name":"Joe"	["\"mgr\":[\"Smith\""]	NULL
NULL	"name":"Jay"	["\"mgr\":[\"Smith\""]	NULL
NULL	"name":"John"	["                      \"tax\":{\"State\":0.5"]	NULL
NULL	"name":"Zac"	["\"mgr\":[\"Smith\""]	NULL
NULL	"name":"Sam"	["\"mgr\":[\"Smith\""]	NULL
NULL	"name":"Ray"	["        \"tax\":{\"State\":0.5"]	NULL
NULL	"name":"Duc"	["                                                                         \"address\":{\"street\":\"Some Street in USA\"}}"]	NULL
Time taken: 0.089 seconds, Fetched: 7 row(s)

Create Databse

CREATE DATABASE finance.db
LOCATION '/user/hive/warehouse/finance'
WITH DBPROPERTIES ('creator'='sam','date'='28-MAR-2016')
;

hive> DESCRIBE DATABASE extended finance_db;
finance_db		hdfs://quickstart.cloudera:8020/user/hive/warehouse/finance	cloudera	USER	{date=28-MAR-2016, creator=sam}

Copy schema/defination but not data
CREATE TABLE IF NOT EXISTS employees2  LIKE employees;

Show Table Like 

show tables  'sale*'


Partitioned, Managed Tables
DROP TABLE sales;
CREATE TABLE sales(
product_id INT,
qty        BIGINT,
amt        DOUBLE
)
PARTITIONED BY (year INT, country STRING)
ROW FORMAT DELIMITED
FIELDS     TERMINATED BY ','
STORED AS  TEXTFILE
LOCATION '/user/hive/warehouse/sales.db/sales'
;

Load Partitioned, Managed Tables from Local file

LOAD DATA LOCAL INPATH '${env:HOME}/sales.2010'
INTO TABLE sales
PARTITION (year=2010,country='USA');

LOAD DATA LOCAL INPATH '${env:HOME}/sales.2010'
INTO TABLE sales
PARTITION (year=2010,country='EU');

hive> dfs -ls -R /user/hive/warehouse/sales.db;
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales/year=2010
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales/year=2010/country=EU
-rwxrwxrwx   1 cloudera supergroup        151 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales/year=2010/country=EU/sales.2010
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales/year=2010/country=USA
-rwxrwxrwx   1 cloudera supergroup        151 2016-03-28 20:11 /user/hive/warehouse/sales.db/sales/year=2010/country=USA/sales.2010

Load Partitioned, Managed Tables from HDFS file ( moves the files from original location)

LOAD DATA  INPATH '/user/poc/txt/sales.2011'
INTO TABLE sales
PARTITION (year=2011,country='USA');

hive> dfs -ls -R /user/hive/warehouse/sales.db;
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:18 /user/hive/warehouse/sales.db/sales
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:15 /user/hive/warehouse/sales.db/sales/year=2010
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:15 /user/hive/warehouse/sales.db/sales/year=2010/country=EU
-rwxrwxrwx   1 cloudera supergroup        151 2016-03-28 20:15 /user/hive/warehouse/sales.db/sales/year=2010/country=EU/sales.2010
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:13 /user/hive/warehouse/sales.db/sales/year=2010/country=USA
-rwxrwxrwx   1 cloudera supergroup        151 2016-03-28 20:13 /user/hive/warehouse/sales.db/sales/year=2010/country=USA/sales.2010
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:18 /user/hive/warehouse/sales.db/sales/year=2011
drwxrwxrwx   - cloudera supergroup          0 2016-03-28 20:18 /user/hive/warehouse/sales.db/sales/year=2011/country=USA
-rwxrwxrwx   1 cloudera supergroup        302 2016-03-28 20:17 /user/hive/warehouse/sales.db/sales/year=2011/country=USA/sales.2011



Analyze Partitioned table for partition

Analyze TABLE sales partition(year=2010,country="USA") compute STATISTICS;

show partitions sales
+-------+---------+-------+--------+------+--------------+-------------------+--------+-------------------+------------------------------------------------------------------------------------------+
| year  | country | #Rows | #Files | Size | Bytes Cached | Cache Replication | Format | Incremental stats | Location                                                                                 |
+-------+---------+-------+--------+------+--------------+-------------------+--------+-------------------+------------------------------------------------------------------------------------------+
| 2010  | EU      | 0     | 1      | 151B | NOT CACHED   | NOT CACHED        | TEXT   | false             | hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales.db/sales/year=2010/country=EU  |
| 2010  | USA     | 10    | 1      | 151B | NOT CACHED   | NOT CACHED        | TEXT   | false             | hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales.db/sales/year=2010/country=USA |
| 2011  | USA     | 0     | 1      | 302B | NOT CACHED   | NOT CACHED        | TEXT   | false             | hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales.db/sales/year=2011/country=USA |
| Total |         | -1    | 3      | 604B | 0B           |                   |        |                   |                                                                                          |
+-------+---------+-------+--------+------+--------------+-------------------+--------+-------------------+------------------------------------------------------------------------------------------+
Fetched 4 row(s) in 5.55s

Non-Partitioned, External Tables
DROP TABLE sales_ext;
CREATE EXTERNAL TABLE sales_ext(
product_id INT,
qty        BIGINT,
amt        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS     TERMINATED BY ','
STORED AS  TEXTFILE
LOCATION '/user/hive/warehouse/sales.db/sales_ext'
;

Partitioned, External Tables
DROP TABLE sales_ext_part;
CREATE EXTERNAL TABLE sales_ext_part(
product_id INT,
qty        BIGINT,
amt        DOUBLE
)
PARTITIONED BY (year INT, country STRING)
ROW FORMAT DELIMITED
FIELDS     TERMINATED BY ','
STORED AS  TEXTFILE
;
To Add partition to Partitioned, External Tables
ALTER TABLE sales_ext_part ADD PARTITION(year=2010,country='USA') LOCATION '/user/poc/txt/2010/';
ALTER TABLE sales_ext_part ADD PARTITION(year=2011,country='USA') LOCATION '/user/poc/txt/2011/';
ALTER TABLE sales_ext_part ADD PARTITION(year=2012,country='USA') LOCATION '/user/poc/txt/2012/';

Analyze TABLE sales_ext_part partition(year=2010,country="USA") compute STATISTICS;
To Change Partition of  External Tables to point to different location ( s3n or hdfs)
ALTER TABLE sales_ext_part PARTITION(year=2010,country='USA') SET LOCATION 'fs://user/poc/txt/arch/2011/' ;
To See  Partition information of table
describe formatted sales_ext_part PARTITION(year=2010,country='USA');

# Detailed Partition Information	 	 
Partition Value:    	[2010, USA]         	 
Database:           	default             	 
Table:              	sales_ext_part      	 
CreateTime:         	Wed Mar 30 17:46:45 PDT 2016	 
LastAccessTime:     	UNKNOWN             	 
Protect Mode:       	None                	 
Location:           	fs://user/poc/txt/arch/2011/	 
Partition Parameters:	 	 
	COLUMN_STATS_ACCURATE	true                
	last_modified_by    	cloudera            
	last_modified_time  	1459388472          
	numFiles            	1                   
	numRows             	10                  
	rawDataSize         	141                 
	totalSize           	151                 
	transient_lastDdlTime	1459388472    
To archive the  Partition 
ALTER TABLE sales_ext_part ARCHIVE PARTITION(year=2012,country='USA');

To Make  Partition undroppable

ALTER TABLE sales_ext_part PARTITION(year=2012,country='USA')  ENABLE NO_DROP;

 Customizing Table Storage Formats

CREATE TABLE kst
PARTITIONED BY (ds string)
ROW FORMAT SERDE 'com.linkedin.haivvreo.AvroSerDe'
WITH SERDEPROPERTIES ('schema.url'='http://schema_provider/kst.avsc')
STORED AS
INPUTFORMAT 'com.linkedin.haivvreo.AvroContainerInputFormat'
OUTPUTFORMAT 'com.linkedin.haivvreo.AvroContainerOutputFormat';


To Make  Bucket Table 
set hive.enforce.bucketing=true;


CREATE TABLE IF NOT EXISTS stocks_bkt (
exchange STRING,
symbol STRING,
ymd STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
volume INT,
price_adj_close FLOAT)
CLUSTERED BY (exchange, symbol)
SORTED BY (ymd ASC)
INTO 96 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/poc/stocks_bkt';

Load Data into Bucket Tables 
INSERT INTO stocks_bkt
SELECT * FROM stock_ext;



Inserting Data into Tables from Queries

INSERT OVERWRITE TABLE Sales PARTITION(year=2015,country='USA')
SELECT * FROM sales_stage;

*** ALTERNATE INSERT - Scan Input Data Once and Split it multiple way into Partition  

FROM staged_employees se
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state = 'OR')
SELECT * WHERE se.cnty = 'US' AND se.st = 'OR'
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state = 'CA')
SELECT * WHERE se.cnty = 'US' AND se.st = 'CA'
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state = 'IL')
SELECT * WHERE se.cnty = 'US' AND se.st = 'IL';


Dynamic Partition Inserts
INSERT OVERWRITE TABLE employees
PARTITION (country = 'US', state)
SELECT ..., se.cnty, se.st
FROM staged_employees se
WHERE se.cnty = 'US';

FROM sales
 INSERT INTO sales_ext_part partition(year,country)
   SELECT product_id,qty,amt,year,country WHERE year=2010;

***Dynamic Partition Inserts WITH ALTERNATE INSERT - Scan Input Data Once and Split it multiple way into Partition  
CREATE TABLE IF NOT EXISTS sales_2010 LIKE sales_ext_part;
CREATE TABLE IF NOT EXISTS sales_2011 LIKE sales_ext_part;

FROM sales s
INSERT INTO TABLE sales_2010 PARTITION(year,country)
SELECT product_id,qty,amt,year,country WHERE year=2010 and country='USA'
INSERT INTO TABLE sales_2011 PARTITION(year,country)
SELECT product_id,qty,amt,year,country WHERE year=2011 and country='USA' ;

Exporting Data

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/hive_sal_export/'
SELECT 'sales_ext_part' tname,spe.*
  FROM sales_ext_part spe
UNION ALL
SELECT 'sales' tname,s.*
  FROM sales s;


Selecting Data from Array collection
SELECT name, subordinates[0] FROM employees

Selecting Data from MAP Element
SELECT name, tax["Stat"] FROM employees;

Selecting Data from STRUCT collectio
SELECT name, address.city FROM employees

Specify Columns with Regular Expressions

SELECT symbol, `price.*` FROM stocks


SELECT upper(name),address.city,salary,
       deductions["Federal Taxes"],
       round(salary * (1 - deductions["Federal Taxes"]))
  FROM emp_txt;


Table generating functions
explode() takes an ONLY array or a map as a parameter

hive> select explode(subordinates) from emp_txt;  => FOR ARRAY
Mary Smith
Todd Jones
Bill King
John Doe
Fred Finance
Stacy Accountant


hive> select explode(deductions) from emp_txt;  ==> FOR MAP Gives with Keys
Federal Taxes	0.2
State Taxes	0.05
Insurance	0.1
Federal Taxes	0.2
State Taxes	0.05
Insurance	0.1
Federal Taxes	0.15
State Taxes	0.03

SELECT parse_url_tuple('http://finance.yahoo.com/q?s=btu', 'HOST', 'PATH', 'QUERY') as (host, path, query)
    >  FROM src;
Host                  Path    Query
finance.yahoo.com	/q	  s=btu


select concat_ws('|',"hi","hello") from src;

hive> select split("Hi Hello How"," ") from src;
OK
["Hi","Hello","How"]

SELECT a.ymd, a.price_close as apple_price, b.price_close as ibm_price
  FROM stocks_bkt a 
  JOIN 
       stocks_bkt b 
    ON a.ymd = b.ymd
 WHERE a.symbol = 'AAPL' 
   AND b.symbol = 'IBM';


When joining three or more tables, if every ON clause uses the same join
key, a single MapReduce job will be used.
For ex.

SELECT a.ymd, a.price_close, b.price_close , c.price_close
  FROM stocks_bkt a 
  JOIN stocks_bkt b ON a.ymd = b.ymd
  JOIN stocks_bkt c ON a.ymd = c.ymd
 WHERE a.symbol = 'AAPL' 
   AND b.symbol = 'IBM' 
    AND c.symbol = 'GE';

 Hive also assumes that the last  table in the query is the largest . It attempts to buffer the
other tables and then stream the last table through, while performing joins on individual
records. Therefore, you should structure your join queries so the largest table is last.


SELECT s.ymd, s.symbol, s.price_close
  FROM stocks s LEFT SEMI JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol;

The reason semi-joins are more efficient than the more general inner join is as follows.
For a given record in the lefthand table, Hive can stop looking for matching records in
the righthand table as soon as any  match is found. At that point, the selected columns
from the lefthand table record can be projected.

Map-side Joins
If all but one table is small, the largest table can be streamed through the mappers while
the small tables are cached in memory. Hive can do all the joining map-side, since it
can look up every possible match against the small tables in memory, thereby eliminating
the reduce step required in the more common join scenarios.

set hive.optimize.bucketmapjoin=true;

 If the bucketed tables actually have the same number of buckets and the data is sorted
by the join/bucket keys, then Hive can perform an even faster sort-merge join. Once
again, properties must be set to enable the optimization:

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;


DISTRIBUTE BY with SORT BY  =  CLUSTER BY
To Partition the Map Reduce Data  

DISTRIBUTE BY works similar to GROUP BY in the sense that it controls how reducers
receive rows for processing, while SORT BY controls the sorting of data inside the reducer.



Index

CREATE INDEX employees_index
ON TABLE employees (country)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD
IDXPROPERTIES ('creator = 'me', 'created_at' = 'some_time')
IN TABLE employees_index_table
PARTITIONED BY (country, name)
COMMENT 'Employees indexed by country and name.';

CREATE INDEX employees_index
ON TABLE employees (country)
AS 'BITMAP'
WITH DEFERRED REBUILD
IDXPROPERTIES ('creator = 'me', 'created_at' = 'some_time')
IN TABLE employees_index_table
PARTITIONED BY (country, name)
COMMENT 'Employees indexed by country and name.';


ALTER INDEX employees_index
ON TABLE employees
PARTITION (country = 'US')
REBUILD

SHOW FORMATTED INDEX ON employees;


CREATE EXTERNAL TABLE sales_ext_part_stg(
product_id INT,
qty        INT,
amt        DOUBLE
)
PARTITIONED BY (day INT)
ROW    FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
;

CREATE TABLE sales_part(
product_id INT,
qty        INT,
amt        DOUBLE
)
PARTITIONED BY(day INT)
ROW    FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS ORC 
LOCATION '/user/hive/warehouse/sales_part'
;

CREATE TABLE sales_part_prqt(
product_id INT,
qty        INT,
amt        DOUBLE
)
PARTITIONED BY(day INT)
ROW    FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS PARQUET 
LOCATION '/user/hive/warehouse/sales_part_prqt'
;

--Setup hive Variable
set hivevar:iday=20160401;


[cloudera@quickstart ~]$ more load_for_day.hql
--Load External Staging Table for a Day
ALTER TABLE sales_ext_part_stg DROP PARTITION(day=${iday});
ALTER TABLE sales_ext_part_stg ADD  PARTITION(day=${iday}) LOCATION '/user/poc/stg/sales_ext_part_stg/${iday}';

set mapreduce.job.reduces=2;

--Load Main Table from External Staging
FROM sales_ext_part_stg stg
INSERT OVERWRITE TABLE sales_part PARTITION(day=${iday})  
SELECT product_id,qty,amt,unit WHERE stg.day=${iday}
INSERT OVERWRITE TABLE sales_part_prqt PARTITION(day=${iday})
SELECT product_id,qty,amt,unit WHERE stg.day=${iday}
;

set mapreduce.job.reduces=4;

--Report
SELECT * FROM (
SELECT '1.sales_ext_part_stg' tname, day, COUNT(*) cnt, SUM(amt) as AMT FROM SALES_EXT_PART_STG  GROUP BY day 
UNION ALL
SELECT '2.sales_part'         tname, day, COUNT(*) cnt, SUM(amt) as AMT FROM SALES_PART          GROUP BY day
UNION ALL
SELECT '3.sales_part_prqt'    tname, day, COUNT(*) cnt, SUM(amt) as AMT FROM SALES_PART_PRQT     GROUP BY day
) as a 
ORDER BY tname,day;

Loading Thru Script
 hive --hiveconf hive.exec.parallel=true --hivevar iday=20160402  -f load_for_day.hql

ALTER TABLE sales_ext_part_stg ADD COLUMNS( unit INT);
ALTER TABLE sales_part ADD COLUMNS( unit INT);
ALTER TABLE sales_part_pqrt ADD COLUMNS( unit INT);

Explain Plan
hive> explain select sum(number) as s from onecol;
OK
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-0 depends on stages: Stage-1

STAGE PLANS:
  Stage: Stage-1
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: onecol
            Statistics: Num rows: 6 Data size: 25 Basic stats: COMPLETE Column stats: NONE
            Select Operator
              expressions: number (type: int)
              outputColumnNames: number
              Statistics: Num rows: 6 Data size: 25 Basic stats: COMPLETE Column stats: NONE
              Group By Operator
                aggregations: sum(number)
                mode: hash
                outputColumnNames: _col0
                Statistics: Num rows: 1 Data size: 8 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  sort order: 
                  Statistics: Num rows: 1 Data size: 8 Basic stats: COMPLETE Column stats: NONE
                  value expressions: _col0 (type: bigint)
      Reduce Operator Tree:
        Group By Operator
          aggregations: sum(VALUE._col0)
          mode: mergepartial
          outputColumnNames: _col0
          Statistics: Num rows: 1 Data size: 8 Basic stats: COMPLETE Column stats: NONE
          File Output Operator
            compressed: false
            Statistics: Num rows: 1 Data size: 8 Basic stats: COMPLETE Column stats: NONE
            table:
                input format: org.apache.hadoop.mapred.TextInputFormat
                output format: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
                serde: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe

  Stage: Stage-0
    Fetch Operator
      limit: -1
      Processor Tree:
        ListSink 

Parallel Execution & Tuning
set hive.exec.parallel=true;
set mapreduce.job.reduces=2;
set mapred.job.reuse.jvm.num.tasks=4;
set hive.exec.compress.intermediate=true; set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec

Strict Mode
 Following things are not allowed in strict mode
1.  queries on partitioned tables are not permitted unless they include a partition filter  in the WHERE  clause
2.  query are those with ORDER BY  clauses, but no LIMIT  clause
3. query prevented is a Cartesian product


Virtual Columns
 Virtual columns are helpful when diagnosing queries where Hive is producing unexpected or null results;

hive> select INPUT__FILE__NAME, BLOCK__OFFSET__INSIDE__FILE,product_id,day,unit from sales_part;
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160403/000000_0	478	1347	20160403	NULL
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160403/000000_0	496	1348	20160403	NULL
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160403/000000_0	514	1349	20160403	NULL
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160403/000000_0	532	13410	20160403	NULL
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	52	1340	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	104	1341	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	157	1342	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	209	1343	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	262	1344	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	314	1345	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	366	1347	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	419	1348	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	471	1349	20160404	10
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sales_part/day=20160404/000000_0	524	13410	20160404	NULL



Enabling Intermediate Compression
 Intermediate compression shrinks the data shuffled between the map and reduce tasks for a job.

set hive.exec.compress.intermediate=true; set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec
set hive.exec.compress.output=true;       set mapred.output.compression.codec    =org.apache.hadoop.io.compress.SnappyCodec



Functions

show functions;

describe function explode;
explode(a) - separates the elements of array a into multiple rows, or the elements of a map into multiple rows and columns 


Table Generating Functions



select array(1,2,3,4,5) from src;
[1,2,3,4,5]

select explode(array(1,2,3,4,5)) as i from src;
1
2
3
4
5

SELECT s ,sub
  FROM src
 LATERAL VIEW exploder(array(1,2,3,4,5)) myid AS sub;


SELECT s ,sub
  FROM src
LATERAL VIEW explode(array(1,2,3,4,5)) myid AS sub;

HELLO	1
HELLO	2
HELLO	3
HELLO	4
HELLO	5

hive> select name,sub
    > from emp_txt
    > LATERAL VIEW explode(subordinates) mgr as sub;
John Doe	Mary Smith
John Doe	Todd Jones
Mary Smith	Bill King
Boss Man	John Doe
Boss Man	Fred Finance
Fred Finance	Stacy Accountant

A UDF for Finding a Zodiac Sign from a Day

package mypkg.hive;

import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hive.ql.exec.UDF;
public class UDFZSign extends UDF{
private SimpleDateFormat df;

//constuctor
public UDFZSign() {
  df = new SimpleDateFormat("MM-dd-yyyy");
  }

  public String evaluate( Date bday){
   return this.evaluate( bday.getMonth(), bday.getDay());
   }

  public String evaluate( String bday){
    Date date= null;
    try{
        date = df.parse(bday);
        } catch ( Exception ex) {
          return null;
         }
     return this.evaluate( date.getMonth(), date.getDay());
    }

   public String evaluate ( Integer month, Integer day ) {
     if ( month ==1) {
        return "Capricorn";
     }
     else {
        return "Aqua";
     }
    
    }
}

hive>  ADD JAR /home/cloudera/UDFZsign.jar;
Added [/home/cloudera/UDFZsign.jar] to class path
Added resources: [/home/cloudera/UDFZsign.jar]

Note that quotes are not required around the JAR file path and currently it needs to be
a full path to the file on a local  filesystem. Hive not only adds this JAR to the classpath,
it puts the JAR file in the distributed cache so it’s available around the cluster.

hive> CREATE TEMPORARY FUNCTION zodiac 
    > AS 'mypkg.hive.UDFZSign';
OK

hive> describe function zodiac;

hive> select name,bday, zodiac(bday) from littlebigdata;

edward capriolo	2-12-1981	Capricorn
bob	           10-10-2004	Aqua
sara connor	4-5-1974	Aqua



hive (hive_db)> show create table twt_ext_part;

createtab_stmt
CREATE EXTERNAL TABLE `twt_ext_part`(
)
COMMENT 'This external Partitioned table stores the flume tweets data which arrives every hour.'
PARTITIONED BY ( 
  `year` int, 
  `month` int, 
  `day` int, 
  `hour` int)
ROW FORMAT SERDE 
  'com.cloudera.hive.serde.JSONSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://nn1.app.com:8020/user/poc/flume/tweets'
TBLPROPERTIES (
  'transient_lastDdlTime'='1459814279')
Time taken: 0.597 seconds, Fetched: 18 row(s)


hive (hive_db)> show create table twt_prqt_part;

createtab_stmt
CREATE TABLE `twt_prqt_part`(
  `id` bigint, 
  `created_at` string, 
  `source` string, 
  `favorited` boolean, 
  `retweet_count` int, 
  `retweeted_status` struct<text:string,user:struct<screen_name:string,name:string>>, 
  `entities` struct<urls:array<struct<expanded_url:string>>,user_mentions:array<struct<screen_name:string,name:string>>,hashtags:array<struct<text:string>>>, 
  `text` string, 
  `user` struct<screen_name:string,name:string,friends_count:int,followers_count:int,statuses_count:int,verified:boolean,utc_offset:int,time_zone:string>, 
  `in_reply_to_screen_name` string)
COMMENT 'Managed Twitter table with PARQUET with Snappy Compression'
PARTITIONED BY ( 
  `year` int, 
  `month` int, 
  `day` int, 
  `hour` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://nn1.app.com:8020/user/poc/hive_db/twt_orc_part'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY', 
  'transient_lastDdlTime'='1459820492')
Time taken: 0.153 seconds, Fetched: 28 row(s)


