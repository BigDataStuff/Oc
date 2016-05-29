//--1.Create generic class
class OcUtil{
  def createToken(str: String): Array[String]={
    str.split('|')
  }
}

//--2.Create Case Class
case class CustomerC(id: Int,name: String,city: String, state: String ="NULL", country: String,credit: Double)

//--3.create Object of Customer
object CustomerO extends OcUtil {
  //Method to map array to customer case class
  def createCustomerClass(strArr: Array[String]) = {
    CustomerC(strArr(0).toInt, strArr(1), strArr(2),"NULL", strArr(3),strArr(4).toDouble)
  }
  //Method to get state for given city
  def getState(city: String): String = city match {
    case "New York" | "NEW YORK" => "NY"
    case "Seattle" => "WA"
    case "Alpharetta" => "GA"
    case "Mumbai" | "Bombay" => "MS"
    case _ => "ZZ"
  }
}

//--4.Customer Data
val customerData= List(("11|Abe|New York|USA|25.25"),
                       ("11|Abe|New York|USA|25.25"),
                       ("12|Bob|Seattle|USA|25.25"),
                       ("13|Sal|Paris|France|75.00"),
                       ("14|Joe|Alpharetta|USA|25.25"),
                       ("15|Sam|Mumbai|India|50.50"),
                       ("16|Joy|NEW YORK|USA|25.25"),
                       ("17|Al|Bombay|India|50.50"),
                       ("18|Zac|Tokyo|Japan|90.99")
                       )

//--Function to Tranform Customer 
def transformCustomer(rdd: org.apache.spark.rdd.RDD[CustomerC]) = {
    rdd.map(x=>(CustomerC(x.id,x.name,x.city,CustomerO.getState(x.city),x.country,x.credit)))
}

//--Spark
val custData = sc.parallelize(customerData,2)                                         //custData: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[1]
//Token and Case Class mapping
val custToken = custData.map(x=>CustomerO.createToken(x))                             //custToken: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2]
val custCls = custToken.map(x=>CustomerO.createCustomerClass(x))                      //custCls: org.apache.spark.rdd.RDD[CustomerC] = MapPartitionsRDD[3]
//Transformations
val custFltr = custCls.filter(x=> x.country=="USA")                                   //custFltr: org.apache.spark.rdd.RDD[CustomerC] = MapPartitionsRDD[4]
val custUniq = custFltr.distinct()                                                    //custUniq: org.apache.spark.rdd.RDD[CustomerC] = MapPartitionsRDD[5]
val custTransf = transformCustomer(custUniq).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER) //custTransf: org.apache.spark.rdd.RDD[CustomerC] = MapPartitionsRDD[6]
//Actions
val custTotal = custTransf.count()    
//custTotal: Long = 4
val custCountByCountry = custTransf.map(x=>(x.country)).countByValue()                
//custCountByCountry: scala.collection.Map[String,Long] = Map(USA -> 4)

//Aggregate Action(Total and Count) - use when input and output has different return type
val custCreditTotalnCount =  custTransf.map(x=>(x.credit)).aggregate((0.0,0))((x,y)=> (x._1 + y,   x._2 +1    ),  //custCreditTotalnCount: (Double, Int) = (101.0,4)
                                                                      (x,y)=> (x._1 + y._1,x._2 + y._2))
//custCreditTotalnCount: (Double, Int) = (101.0,4)
val custCreditAvg = custCreditTotalnCount._1 / custCreditTotalnCount._2                                           
//custCreditAvg: Double = 25.25

//Transformations ( Actions Returing RDDs)
val custByLetter = custTransf.map(x=>(x.name)).groupBy(x=>x.charAt(0))                //custByLetter: org.apache.spark.rdd.RDD[(Char, Iterable[String])] = ShuffledRDD[6]
custByLetter.collect.foreach(println)
//(B,CompactBuffer(Bob))
//(J,CompactBuffer(Joe, Joy))
//(A,CompactBuffer(Abe))

val custCountByState = custTransf.map(x=>(x.state,1)).reduceByKey((x,y)=>x+y)         //custCountByState: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[7] 
custCountByState.collect.foreach(println)
//(GA,1)
//(WA,1)
//(NY,2)

val custListByState = custTransf.map(x=>(x.state,x.name)).groupByKey()                //custListByState: org.apache.spark.rdd.RDD[(String, Iterable[String])] = ShuffledRDD[8]
custListByState.collect.foreach(println)
//(GA,CompactBuffer(Joe))
//(WA,CompactBuffer(Bob))
//(NY,CompactBuffer(Abe, Joy))



