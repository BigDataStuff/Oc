//--2.Create Case Class
case class OrderC(tranId: Long, date: String, dt: Int, timeStamp: Long, orderId: Long, custId: Int, prodId: Short, qty: Float, price: Float, amt:Double)

//--3.create Object of Order
object OrderO extends OcUtil {
  //Method to map array to customer case class
  def createOrderClass(strArr: Array[String]) = {
    OrderC(strArr(0).toLong, strArr(1), strArr(2).toInt, strArr(3).toLong, strArr(4).toLong, strArr(5).toInt, strArr(6).toShort, strArr(7).toFloat,strArr(8).toFloat,strArr(9).toDouble)
  }
}

//--4.Order Data
val orderData= List(
("1|Wed May 25 13:35:15 EDT 2016|20160525|1463938515|1|11|21|1|1.50|10"),
("2|Wed May 25 13:35:17 EDT 2016|20160525|1463938517|1|11|22|2|2.50|20"),
("3|Wed May 25 13:35:19 EDT 2016|20160525|1463938519|1|11|23|3|3.50|30"), 
("4|Wed May 25 13:35:21 EDT 2016|20160525|1463938521|1|11|24|4|4.50|40"),
("5|Wed May 25 13:35:23 EDT 2016|20160525|1463938523|1|11|25|5|5.50|50"),
("6|Wed May 25 13:35:15 EDT 2016|20160525|1463938515|2|12|21|1|1.50|10"),
("7|Wed May 25 13:35:17 EDT 2016|20160525|1463938517|2|12|22|2|2.50|20"),
("8|Wed May 25 13:35:19 EDT 2016|20160525|1463938519|2|12|23|3|3.50|30"), 
("9|Wed May 25 13:35:21 EDT 2016|20160525|1463938521|2|12|24|4|4.50|40"),
("10|Wed May 25 13:35:15 EDT 2016|20160525|1463938515|3|13|22|1|1.50|10"),
("11|Wed May 25 13:35:17 EDT 2016|20160525|1463938517|3|13|23|2|2.50|20"),
("12|Wed May 25 13:35:19 EDT 2016|20160525|1463938519|3|13|24|3|3.50|30"), 
("13|Wed May 25 13:35:15 EDT 2016|20160525|1463938515|4|14|24|1|1.50|10"),
("14|Wed May 25 13:35:17 EDT 2016|20160525|1463938517|4|14|25|2|2.50|20"),
("15|Thr May 26 13:35:15 EDT 2016|20160526|1463938515|5|11|21|1|1.50|10"),
("16|Thr May 26 13:35:17 EDT 2016|20160526|1463938517|5|11|22|2|2.50|20"),
("17|Thr May 26 13:35:19 EDT 2016|20160526|1463938519|5|11|23|3|3.50|30"), 
("18|Thr May 26 13:35:21 EDT 2016|20160526|1463938521|5|11|24|4|4.50|40"),
("19|Thr May 26 13:35:15 EDT 2016|20160526|1463938515|6|12|21|1|1.50|10"),
("20|Thr May 26 13:35:17 EDT 2016|20160526|1463938517|6|12|22|2|2.50|20"),
("21|Thr May 26 13:35:19 EDT 2016|20160526|1463938519|6|12|23|3|3.50|30"), 
("22|Thr May 26 13:35:15 EDT 2016|20160526|1463938515|7|13|22|1|1.50|10"),
("23|Thr May 26 13:35:17 EDT 2016|20160526|1463938517|7|13|23|2|2.50|20"),
("24|Thr May 26 13:35:15 EDT 2016|20160526|1463938515|8|20|30|9|9.50|90"),
("25|Fri May 27 13:35:15 EDT 2016|20160527|1463938515|9|11|21|1|1.50|10"),
("26|Fri May 27 13:35:17 EDT 2016|20160527|1463938517|9|11|22|2|2.50|20"),
("27|Fri May 27 13:35:19 EDT 2016|20160527|1463938519|9|11|23|3|3.50|30"), 
("28|Fri May 27 13:35:15 EDT 2016|20160527|1463938515|10|12|21|1|1.50|10"),
("29|Fri May 27 13:35:17 EDT 2016|20160527|1463938517|10|12|22|2|2.50|20"),
("30|Fri May 27 13:35:15 EDT 2016|20160527|1463938515|11|13|22|1|1.50|10"),
("31|Fri May 27 13:35:17 EDT 2016|20160527|1463938517|11|13|23|2|2.50|20"),
("32|Fri May 27 13:35:15 EDT 2016|20160527|1463938515|12|20|30|9|9.50|90")
)

//--Spark
val ordData = sc.parallelize(orderData,2)                                         
//Token and Case Class mapping
val ordToken = ordData.map(x=>OrderO.createToken(x))                             
val ordCls = ordToken.map(x=>OrderO.createOrderClass(x))                       
//Transformations
val ordFltr = ordCls.filter(x=> x.custId!=13).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER) 
//Actions
val ordTotal = ordFltr.count()    
//ordTotal: Long = 25                                                             
val ordCountByDate = ordFltr.map(x=>(x.dt)).countByValue()   
//ordCountByDate: scala.collection.Map[Int,Long] = Map(20160526 -> 8, 20160525 -> 11, 20160527 -> 6)
             
//Transformations ( Actions Returing RDDs)
val saleByCust = ordFltr.map(x=>(x.custId,x.amt)).reduceByKey((x,y)=> x + y) //saleByCust: org.apache.spark.rdd.RDD[(Int, Double)] = ShuffledRDD[249]
saleByCust.collect.foreach(println)
//(14,30.0)                                                                       
//(20,180.0)
//(12,190.0)
//(11,310.0)

//Option#1(Expensive)-Transformations ( Actions Returing RDDs) GroupByKey 
val prodByCust1 = ordFltr.map(x=>(x.custId,x.prodId)).distinct.groupByKey().sortByKey().map(x=>(x._1,x._2.toList.sorted)) //prodByCust1: org.apache.spark.rdd.RDD[(Int, List[Short])] = MapPartitionsRDD[258]
prodByCust1.collect.foreach(println)
//(11,List(21, 22, 23, 24, 25))                                                   
//(12,List(21, 22, 23, 24))
//(14,List(24, 25))
//(20,List(30))

//Option#2(Generic)Transformations ( Actions Returing RDDs) aggregateByKey 
val prodByCust2 = ordFltr.map(x=>(x.custId,x.prodId)).distinct.aggregateByKey("")((x,y)=>x+"/"+y,(x,y)=> x+y).sortByKey() //prodByCust2: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[266]
prodByCust2.collect.foreach(println)
//(11,/23/24/25/22/21)                                                            
//(12,/23/24/22/21)
//(14,/24/25)
//(20,/30)

//Option#1(Expensive) -Transformations ( Actions Returing RDDs) mapValues & reduceByKey - To calculate avg
val saleNnoOfProdByCust1 = ordFltr.map(x=>(x.custId,x.amt)).mapValues(x=>(x,1)).reduceByKey((x,y)=> (x._1 + y._1,x._2+y._2)) //saleNnoOfProdByCust1: org.apache.spark.rdd.RDD[(Int, (Double, Int))] = ShuffledRDD[269]
saleNnoOfProdByCust1.collect.foreach(println)
//(14,(30.0,2))
//(20,(180.0,2))
//(12,(190.0,9))
//(11,(310.0,12))

//Option#1(Generic) Transformations ( Actions Returing RDDs) combineByKey - To calculate avg
val saleNnoOfProdByCust2 = ordFltr.map(x=>(x.custId,x.amt)).combineByKey(
                                 (v)=> (v,1),
                                 (x:(Double,Int),v)=>(x._1 + v,x._2+1 ),
                                 (a:(Double,Int),b:(Double,Int))=>(a._1+b._1,a._2+b._2)
                                 )                                                        //saleNnoOfProdByCust2: org.apache.spark.rdd.RDD[(Int, (Double, Int))] = ShuffledRDD[271]
saleNnoOfProdByCust2.collect.foreach(println)
//(14,(30.0,2))
//(20,(180.0,2))
//(12,(190.0,9))
//(11,(310.0,12))



