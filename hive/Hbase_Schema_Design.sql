create_namespace 'oc'
--Create Hbase table with 3 ColumnFamily and Different Versions
create 'oc:cust', {NAME =>'c',VERSIONS => 1}, {NAME =>'a',VERSIONS => 2}, {NAME => 'o' ,VERSIONS => 3} 

describe 'oc:cust'

--Record Cust at with 
put 'oc:cust', 'a1,o1', 'c:name',    'Abe'
put 'oc:cust', 'a1,o1', 'c:city',    'Bern'

put 'oc:cust', 'a1,o1', 'a:credit',  '500.50'
put 'oc:cust', 'a1,o1', 'a:credit',  '1500.50'
put 'oc:cust', 'a1,o1', 'a:debit',   '5000.50'

---
get 'oc:cust', 'a1,o1', {COLUMN => ['c','a','o'],VERSIONS => 2}
--COLUMN                                                     CELL                                                                                                                                                                      
-- a:credit                                                  timestamp=1462801680910, value=1500.50                                                                                                                                    
-- a:credit                                                  timestamp=1462801680890, value=500.50                                                                                                                                     
-- a:debit                                                   timestamp=1462801682030, value=5000.50                                                                                                                                    
-- c:city                                                    timestamp=1462801658625, value=Bern                                                                                                                                       
-- c:name                                                    timestamp=1462801657454, value=Abe   

put 'oc:cust', 'a1,o1', 'o:pid1',     'p1'
put 'oc:cust', 'a1,o1', 'o:qty1',     '7'
put 'oc:cust', 'a1,o1', 'o:price1',   '19.99'
put 'oc:cust', 'a1,o1', 'o:pname1',    'Hadoop Book'
put 'oc:cust', 'a1,o1', 'o:pcat1',     'Books'

put 'oc:cust', 'a1,o1', 'o:pid2',      'p2'
put 'oc:cust', 'a1,o1', 'o:qty2',      '10'
put 'oc:cust', 'a1,o1', 'o:price2',    '9.99'
put 'oc:cust', 'a1,o1', 'o:pname2',    'Hive Book'
put 'oc:cust', 'a1,o1', 'o:pcat2',     'Books'

get 'oc:cust', 'a1,o1', {COLUMN => ['c','a','o'],VERSIONS => 1}
--COLUMN                                                     CELL                                                                                                                                                                      
-- a:credit                                                  timestamp=1462801680910, value=1500.50                                                                                                                                    
-- a:debit                                                   timestamp=1462801682030, value=5000.50                                                                                                                                    
-- c:city                                                    timestamp=1462801658625, value=Bern                                                                                                                                       
-- c:name                                                    timestamp=1462801657454, value=Abe                                                                                                                                        
-- o:pcat1                                                   timestamp=1462801777174, value=Books                                                                                                                                      
-- o:pcat2                                                   timestamp=1462801778828, value=Books                                                                                                                                      
-- o:pid1                                                    timestamp=1462801776758, value=p1                                                                                                                                         
-- o:pid2                                                    timestamp=1462801777327, value=p2                                                                                                                                         
-- o:pname1                                                  timestamp=1462801777073, value=Hadoop Book                                                                                                                                
-- o:pname2                                                  timestamp=1462801777597, value=Hive Book                                                                                                                                  
-- o:price1                                                  timestamp=1462801776966, value=19.99                                                                                                                                      
-- o:price2                                                  timestamp=1462801777502, value=9.99                                                                                                                                       
-- o:qty1                                                    timestamp=1462801776875, value=7                                                                                                                                          
 --o:qty2                                                    timestamp=1462801777403, value=10      

put 'oc:cust', 'a1,o2', 'c:name',    'Abe'
put 'oc:cust', 'a1,o2', 'c:city',    'Bern'

put 'oc:cust', 'a1,o2', 'a:credit',  '300.50'
put 'oc:cust', 'a1,o2', 'a:debit',   '3500.50'

put 'oc:cust', 'a1,o2', 'o:pid1',     'p3'
put 'oc:cust', 'a1,o2', 'o:qty1',     '7'
put 'oc:cust', 'a1,o2', 'o:price1',   '29.99'
put 'oc:cust', 'a1,o2', 'o:pname1',    'Spark Book'
put 'oc:cust', 'a1,o2', 'o:pcat1',     'Books'

get 'oc:cust', 'a1,o2', {COLUMN => ['c','a','o'],VERSIONS => 1}
--COLUMN                                                     CELL                                                                                                                                                                      
-- a:credit                                                  timestamp=1462801954334, value=300.50                                                                                                                                     
-- a:debit                                                   timestamp=1462801954401, value=3500.50                                                                                                                                    
-- c:city                                                    timestamp=1462801954205, value=Bern                                                                                                                                       
-- c:name                                                    timestamp=1462801954098, value=Abe                                                                                                                                        
-- o:pcat1                                                   timestamp=1462801955792, value=Books                                                                                                                                      
-- o:pid1                                                    timestamp=1462801954483, value=p3                                                                                                                                         
-- o:pname1                                                  timestamp=1462801954655, value=Spark Book                                                                                                                                 
-- o:price1                                                  timestamp=1462801954596, value=29.99                                                                                                                                      
-- o:qty1                                                    timestamp=1462801954555, value=7     

--Rec 2
put 'oc:cust', 'b1,o1', 'c:name',    'beb'
put 'oc:cust', 'b1,o1', 'c:city',    'Rome'

put 'oc:cust', 'b1,o1', 'a:credit',  '400.50'
put 'oc:cust', 'b1,o1', 'a:debit',   '4500.50'

put 'oc:cust', 'b1,o1', 'o:pid1',     'p1'
put 'oc:cust', 'b1,o1', 'o:qty1',     '18'
put 'oc:cust', 'b1,o1', 'o:price1',   '5.99'
put 'oc:cust', 'b1,o1', 'o:pname1',    'Scala Book'
put 'oc:cust', 'b1,o1', 'o:pcat1',     'Functional Programming'

--Rec3 with 3 Version of Product
put 'oc:cust', 'c1,o10', 'c:name',    'Rosul'
put 'oc:cust', 'c1,o10', 'c:city',    'Paris'

put 'oc:cust', 'c1,o10', 'a:credit',  '700.50'
put 'oc:cust', 'c1,o10', 'a:debit',   '7500.50'

put 'oc:cust', 'c1,o10', 'o:pid1',     'p1241'
put 'oc:cust', 'c1,o10', 'o:qty1',     '18'
put 'oc:cust', 'c1,o10', 'o:price1',   '5.99'
put 'oc:cust', 'c1,o10', 'o:pname1',   'Mac Book'
put 'oc:cust', 'c1,o10', 'o:pcat1',    'Electronic'

put 'oc:cust', 'c1,o10', 'o:pid1',     'p1241'
put 'oc:cust', 'c1,o10', 'o:qty1',     '18'
put 'oc:cust', 'c1,o10', 'o:price1',   '7.99'
put 'oc:cust', 'c1,o10', 'o:pname1',   'Mac Book'
put 'oc:cust', 'c1,o10', 'o:pcat1',    'Electronic'

put 'oc:cust', 'c1,o10', 'o:pid1',     'p1241'
put 'oc:cust', 'c1,o10', 'o:qty1',     '18'
put 'oc:cust', 'c1,o10', 'o:price1',   '99.99'
put 'oc:cust', 'c1,o10', 'o:pname1',   'Mac Book'
put 'oc:cust', 'c1,o10', 'o:pcat1',    'Electronic'

get 'oc:cust', 'c1,o10', {COLUMN => ['c','a','o'],VERSIONS => 1}


--Adding Record Thru Scala/Spark
--Normal Load using org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.fs.Path;

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
 
val conf = HBaseConfiguration.create()
val tableName = "oc:cust"
conf.set(TableInputFormat.INPUT_TABLE, tableName)
 
val myTable = new HTable(conf, tableName);
val p = new Put(new String("d1,o11").getBytes());
p.add("c".getBytes(), "name".getBytes(), new String("Dude").getBytes());
p.add("c".getBytes(), "city".getBytes(), new String("Delhi").getBytes());
p.add("a".getBytes(), "credit".getBytes(), new String("8000").getBytes());
p.add("a".getBytes(), "debit".getBytes(), new String("1000").getBytes());
p.add("o".getBytes(), "pid1".getBytes(), new String("p12").getBytes());
p.add("o".getBytes(), "qty1".getBytes(), new String("1000").getBytes());
p.add("o".getBytes(), "price1".getBytes(), new String("399.99").getBytes());
p.add("o".getBytes(), "price1".getBytes(), new String("499.99").getBytes());
p.add("o".getBytes(), "pcat1".getBytes(), new String("Electronic").getBytes());

myTable.put(p);
myTable.flushCommits();


--Reading Record Thru Scala/Spark
--using newAPIHadoopRDD
import org.apache.spark._
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue

val conf = HBaseConfiguration.create()
val tableName ="oc:cust"
conf.set(TableInputFormat.INPUT_TABLE, tableName)
val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

println("Number of Records found : " + hBaseRDD.count())

import scala.collection.JavaConverters._

//--Option#1
hBaseRDD.map(tuple => tuple._2).map(x => x.getColumn("c".getBytes(), "name".getBytes())).map(kv=> {kv.asScala.reduceLeft{(a,b)=> if (a.getTimestamp > b.getTimestamp) a else b}.getValue })

//--Option#2
 hBaseRDD.map(tuple => tuple._2).map(x => x.raw()).map(f => KeyValueToString(f)).saveAsTextFile(sink)

------
def KeyValueToString(keyValues: Array[KeyValue]): String = {
    var it = keyValues.iterator
    var res = new StringBuilder
    while (it.hasNext) {
      res.append( Bytes.toString(it.next.getValue()) + ",")
    }
    res.substring(0, res.length-1);
}
-----

case class KeyValue(key: String, value: Integer)

def KeyValueToString(keyValues: Array[KeyValue]): String = {
    var it = keyValues.iterator
    var res = new StringBuilder
    while (it.hasNext) {
      res.append( it.next() )
    }
    res.substring(0, res.length-1) 
}

def KeyValueToString(keyValues: Array[KeyValue]): String = {
    var it = keyValues.iterator
    var res = new StringBuilder
    while (it.hasNext) {
      res.append( Bytes.toString(it.next() ))
    }
    res.substring(0, res.length-1) 
}


