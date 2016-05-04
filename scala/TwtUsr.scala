import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object TwtUsr {
def main(args: Array[String]) {
val conf    = new SparkConf().setAppName("Tweeter User")
val sc      = new SparkContext(conf)
val hiveCtx = new HiveContext(sc)
val thdf    = hiveCtx.read.format("parquet").load("/user/poc/hive_db/twt_orc_part/")
thdf.registerTempTable("twt")
val tresult = hiveCtx.sql("SELECT DISTINCT user.screen_name, user.name FROM twt");
tresult.saveAsParquetFile("/user/poc/txt/twtusr")
val kv = tresult.rdd.map( x => (x.getString(0),x.getString(1)));
val cnt = kv.count
println("No of Users:- %s" , cnt)
 }
}
