import org.apache.spark.graphx.{Graph => Graph1, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main ( args: Array[String] ) {
  val conf = new SparkConf().setAppName("Undirected Graph")
  val sc = new SparkContext(conf)
  val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map(line => { val(node,neighbours) = line.split(",").splitAt(1)
  												(node(0).toLong,neighbours.toList.map(_.toLong))})
												.flatMap( x => x._2.map(y => (x._1, y)))
  												.map(a => Edge(a._1, a._2, a._1))
  			
  val graph: Graph1[Long, Long] = Graph1.fromEdges(edges, "defaultProperty")
									   .mapVertices((id, _) => id)
  
  val group = graph.pregel(Long.MaxValue, 5) (
  (id, m, n) => math.min(m, n),
  triplet => {
  if(triplet.attr < triplet.dstAttr) {
  Iterator((triplet.dstId, triplet.attr))
  }else if (triplet.srcAttr < triplet.attr) {
  Iterator ((triplet.dstId, triplet.srcAttr))
  } else {
  Iterator.empty
  }
  },
  (a,b) => math.min (a,b)
  )
  
  val res = group.vertices.map(graph => (graph._2, 1))
  						.reduceByKey(_+_)
  						.sortByKey()
  						.map( k => k._1.toString+","+k._2.toString )
  						
  println("Connected graph sizes: ");
  res.collect().foreach(println)
  
  }
}
 
