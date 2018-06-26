package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
object SetSimJoin {
  def main(args: Array[String]) {
  val inputFile = args(0)
  val outputFolder = args(1)
  val threshold = args(2).toDouble
  val conf = new SparkConf().setAppName("SetSimJoin")//.setMaster("local")
  val sc = new SparkContext(conf)
  // split the data by " "
  val text = sc.textFile(inputFile).map(x => x.split(" "))
  val pairs = text.flatMap(line => 
{
  // distinguish the recordID with elementID list
  val record_id = line(0) //the data in first index is recordID
  val elements_list = line.drop(1) // the rest data is the list of elementID
  val Int_elements = elements_list.map(_.toInt)// convert array[string] to array[int]
  val elements = Int_elements.sorted//sorted the tokens  
  // partition using prefixes, make one of the elements as Key and (recordID,length of current_list,element_list) as Value
  val partition = elements.groupBy(identity).mapValues(_.size)//Traverse the rdd and map into (one element, count)
  val length = elements.length
  val prefix = length - Math.ceil(threshold * length) + 1//Prefix filter principle
  val index = (prefix -1).toInt//get the index of the max value
  val value = elements(index)//get the critical value
  partition.map({ case(one, count) => (one.toInt, Array((record_id.toInt,length.toDouble, elements))) })
  .filter(f=> f._1.toInt <= value.toInt)//prefix filter
})
  val combine = pairs.reduceByKey(_++_)
  .filter(f => f._2.length > 1)
  .flatMap( {case(one, l) => 
  //permutations and combinations: get various possibilities of two element_lists which have similarity
  l.toList.combinations(2)
  //emit pair as (record1,elements),(record2,elements),min_length,max_length)
  .map(x => ((x(0)._1,x(0)._3), (x(1)._1, x(1)._3), Math.min(x(0)._2.toDouble,x(1)._2.toDouble),Math.max(x(1)._2.toDouble,x(0)._2.toDouble)))
  //length filter
  .filter(f => f._3/f._4 >= threshold)
})
  //calculate the similarity: 
  val similarity = combine.map(x => ((x._1._1,x._2._1), (((x._1._2.toSet)&(x._2._2.toSet)).size).toDouble/(((x._1._2.toSet)++(x._2._2.toSet)).size).toDouble))
 .filter(r => r._2 >= threshold)//similarity >= threshold
  val duplicates = similarity.reduceByKey((a,b)=>a)// remove duplicates
  val results = duplicates.sortBy(f => (f._1._1,f._1._2))//sort
  val result = results.map(x => ((x._1._1,x._1._2)+ "\t" + x._2))
  result.saveAsTextFile(outputFolder)
}
}