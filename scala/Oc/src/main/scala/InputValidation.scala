object InputValidation {
  var files: String = " "
  var topn: Int = 0

  //Validate Given Input
  def validateInput(param: Array[String]) {
    if (param.length == 0) {
      files = "/user/poc/flume/tweets/*/*/*/*/FlumeData*"
      topn = 10
    }
    else {
      files = param(0)
      topn = param(1).toInt
    }

    println("-------------------------------")
    println("No of Parameters: " + param.length)
    println("Files : " + files)
    println("Top N : " + topn)
    println("-------------------------------")
  }

  //File Processing
  def findTopHashTags(files: String, topn: Int): Unit = {
    //val conf = new SparkConf().setAppName("Top HashTags#")
    //val sc = new SparkContext(conf)
    val twts = sc.textFile(files, 8)
    val topTwts = twts.flatMap(x => x.split(","))
    .filter(x => x.contains("\"hashtags\""))
    .filter(x => x.contains("\"text\""))
    .flatMap(x => x.split(":"))
    .filter(x => !(x.contains("\"hashtags\"")))
    .filter(x => !(x.contains("\"text\"")))
    .map(x => (x, 1))
    .reduceByKey((x, y) => x + y)
    .map(x => x.swap)
    .sortByKey(false)
    .top(topn)
    .map(x => x.swap)
    .toSeq
    println("---------------------------------------------")
    println("Given Files :- ", files)
    println("---------------------------------------------")
    for (i <- topTwts) {
      println(i._1 + " [" + i._2 + "]")
     }
    println("---------------------------------------------")
}
  //Main Method
  def main(args: Array[String]) {
    InputValidation.validateInput(args)
    findTopHashTags(files,topn.toInt)
  }
}

