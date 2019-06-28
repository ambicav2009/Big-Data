import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply {
  def main(args: Array[ String ]) {
	val conf = new SparkConf().setAppName("Matrices multiplication")
	val sc = new SparkContext(conf)

	val matrix_m = sc.textFile(args(0)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )	

	val matrix_n = sc.textFile(args(1)).map(line => { 
							val readLine = line.split(",")
							(readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
						} )

	val multiply = matrix_m.map( matrix_m => (matrix_m._2, matrix_m)).join(matrix_n.map( matrix_n => (matrix_n._1,matrix_n)))
									 .map{ case (k, (matrix_m,matrix_n)) => 
										((matrix_m._1,matrix_n._2),(matrix_m._3 * matrix_n._3)) }

	val reduceValues = multiply.reduceByKey((x,y) => (x+y))

	val result = reduceValues.sortByKey(true, 0)
		
	result.saveAsTextFile(args(2))

	sc.stop()
	
  }
}
