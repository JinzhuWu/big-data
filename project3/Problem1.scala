package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Problem1 {
  def main(args: Array[String]) {
  val inputFile = args(0)
  val outputFolder = args(1)
  val conf = new SparkConf().setAppName("Problem1").setMaster("local")
  val sc = new SparkContext(conf)
  // read textfile
  val input =  sc.textFile(inputFile)
  // get the data of index1 and index3
  val pair = input.map(line => (line.split(" ")(1).toInt, line.split(" ")(3).toDouble))
  // map the values into distance and 1, which is used for counting
  val node = pair.mapValues(x => (x,1))
  // sum the counter and distance, we can get Average distance. And then sorting the result.
  val avgLen = node.reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).mapValues(x => (x._1.toDouble/x._2)).sortBy(_._2, false)
  val result = avgLen.map(x => x._1+ "\t" +x._2)
  result.saveAsTextFile(outputFolder)
}
}