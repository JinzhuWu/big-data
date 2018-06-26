package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId

object Problem2 {
  def main(args: Array[String]) {
  val inputFile = args(0)
  val conf = new SparkConf().setAppName("Problem2").setMaster("local")
  val sc = new SparkContext(conf)
  val input =  sc.textFile(inputFile)

//create the edgesRDD, which is a pair with two nodes. 
//read the data in index1 and index2
  val edgesRDD: RDD[(VertexId, VertexId)] = input.map(line => line.split(" "))
      .map(line =>
        (line(1).toLong, line(2).toLong))
//create the graph by "graph from edges" method
  val graph = Graph.fromEdgeTuples(edgesRDD, 1)
//read the target node
  val sourceId = args(1).toDouble
//create initialgraph, the distance to target node is 0, and others are initially INFs
  val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

  val sssp = initialGraph.pregel(Double.PositiveInfinity)(

  // Vertex Program
  (id, dist, newDist) => math.min(dist, newDist),

  // Send Message
  triplet => {
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  //Merge Message
  (a, b) => math.min(a, b))
// filter the result whose distance is still INF, it means this node is not reachable
val result = sssp.vertices.filter(_._2 != Double.PositiveInfinity)
//we still need to remove the source id
println(result.count - 1)
}
}