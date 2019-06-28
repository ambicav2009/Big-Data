import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@SerialVersionUID(123L)
case class Vertex (tag: Long, group: Long, vid: Long, adjacent:List[String]) extends Serializable {}

object Graph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    val firstm = sc.textFile(args(0)).map( line => {
        var input = line.split(",");
        var vid = input(0).toLong;
        var tag = 0.toLong;
        var op = input.toList.tail;
        Vertex(tag,vid,vid,op)
  })
    var firstr = firstm.map( firstm => (firstm.vid,firstm) )
    .map { case (k,firstm) =>
        var f_tag = firstm.tag;
        var f_vid = firstm.vid;
        var f_group = firstm.group;
        var f_ad = firstm.adjacent;
        (f_vid,Vertex(f_tag,f_group,f_vid,f_ad)) }
   
       var secondm1 = firstr.values.map( firstr => (firstr.vid,firstr))
       var secondm2 = firstr.flatMap(firstr => for (j <- firstr._2.adjacent)yield{
           (j.toLong,new Vertex(1.toLong,firstr._2.group,0.toLong,List(0.toString)))
           })
       var secondm = secondm1.union(secondm2).groupByKey()

       var secondr = secondm.map(secondm =>
       {
           var max = Long.MaxValue;
           var list1 : List[String] = List()
           for (i <- secondm._2){
               if (i.tag == 0){
                   list1 = i.adjacent
               }
           if (max > i.group)
           {
               max = i.group
           }

           }
           (max,Vertex(0.toLong,max,secondm._1,list1))
           })

       firstr=secondr
    secondm1 = firstr.values.map( firstr => (firstr.vid,firstr))
       secondm2 = firstr.flatMap(firstr => for (j <- firstr._2.adjacent)yield{
           (j.toLong,new Vertex(1.toLong,firstr._2.group,0.toLong,List(0.toString)))
           })
       secondm = secondm1.union(secondm2).groupByKey()

       secondr = secondm.map(secondm =>
       {
           var max = Long.MaxValue;
           var list1 : List[String] = List()
           for (i <- secondm._2){
               if (i.tag == 0){
                   list1 = i.adjacent
               }
           if (max > i.group)
           {
               max = i.group
           }

           }
           (max,Vertex(0.toLong,max,secondm._1,list1))
           })
       
       firstr=secondr
       secondm1 = firstr.values.map( firstr => (firstr.vid,firstr))
       secondm2 = firstr.flatMap(firstr => for (j <- firstr._2.adjacent)yield{
           (j.toLong,new Vertex(1.toLong,firstr._2.group,0.toLong,List(0.toString)))
           })
       secondm = secondm1.union(secondm2).groupByKey()

       secondr = secondm.map(secondm =>
       {
           var max = Long.MaxValue;
           var list1 : List[String] = List()
           for (i <- secondm._2){
               if (i.tag == 0){
                   list1 = i.adjacent
               }
           if (max > i.group)
           {
               max = i.group
           }

           }
           (max,Vertex(0.toLong,max,secondm._1,list1))
           })
       
       firstr=secondr
       secondm1 = firstr.values.map( firstr => (firstr.vid,firstr))
       secondm2 = firstr.flatMap(firstr => for (j <- firstr._2.adjacent)yield{
           (j.toLong,new Vertex(1.toLong,firstr._2.group,0.toLong,List(0.toString)))
           })
       secondm = secondm1.union(secondm2).groupByKey()

       secondr = secondm.map(secondm =>
       {
           var max = Long.MaxValue;
           var list1 : List[String] = List()
           for (i <- secondm._2){
               if (i.tag == 0){
                   list1 = i.adjacent
               }
           if (max > i.group)
           {
               max = i.group
           }   }
           (max,Vertex(0.toLong,max,secondm._1,list1))
           })
       firstr=secondr
       secondm1 = firstr.values.map( firstr => (firstr.vid,firstr))
       secondm2 = firstr.flatMap(firstr => for (j <- firstr._2.adjacent)yield{
           (j.toLong,new Vertex(1.toLong,firstr._2.group,0.toLong,List(0.toString)))
           })
       secondm = secondm1.union(secondm2).groupByKey()
       secondr = secondm.map(secondm =>
       {
           var max = Long.MaxValue;
           var list1 : List[String] = List()
           for (i <- secondm._2){
               if (i.tag == 0) {    list1 = i.adjacent        }
           if (max > i.group)    {    max = i.group        }
    }
           (max,Vertex(0.toLong,max,secondm._1,list1))
           })
       var map3 = secondr.map(secondr => (secondr._1,1.toLong)).reduceByKey(_+_)
       map3.collect.foreach(println)
    sc.stop()
}}
