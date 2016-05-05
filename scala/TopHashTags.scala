import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TopHashTags{
  def main(args: Array[String]){
    val conf  = new SparkConf().setAppName("Top HashTags#")
    val sc    = new SparkContext(conf)
    var topn  = 100
    var files = "/user/poc/flume/tweets/*/*/*/*/FlumeData*"
    println(args.length)
    if ( args.length ==0 ){
          var files = "/user/poc/flume/tweets/*/*/*/*/FlumeData*"
    }
    else{
          var files = args(0)
          var topn  = args(1)
    }
    val twts = sc.textFile(files,8)
    val topTwts = twts.flatMap(x=> x.split(","))
                 .filter(x=>x.contains("\"hashtags\""))
                 .filter(x=>x.contains("\"text\""))
                 .flatMap(x=>x.split(":"))
                 .filter(x=> !(x.contains("\"hashtags\"")))
                 .filter(x=> !(x.contains("\"text\"")))
                 .map(x=>(x,1))
                 .reduceByKey((x,y)=>x+y)
                 .map(x=>x.swap)
                 .sortByKey(false)
                 .top(topn)
                 .map(x=>x.swap)
                 .toSeq
                 
    println("---------------------------------------------")
    println("Given Files :- ",files)
    println("---------------------------------------------")
    for( i <- topTwts){
        println(i._1+" ["+i._2+"]")
    }
    println("---------------------------------------------")
  }
}
