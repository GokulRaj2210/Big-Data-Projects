import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer


object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
         = sc.textFile(args(0))
             .map( line => {val a = line.split(",")
                           (a(1).toInt,a(0).toInt) })

    var R: RDD[(Int,Int)]             
         = graph.groupByKey()
           .map{ case(keyval,valueval)=> if(keyval==1 || keyval == start_id)                                                          											{
                                          (keyval,0)}
					else{	
					(keyval,max_int)					
					}
		}
										
   
      
for(i <- 0 until iterations)
{		 
       R = R.join(graph)
            .flatMap( v => { 
			var k = new ListBuffer[(Int,Int)]
			if((v._2._1)!=max_int)
			{k+=((v._2._2,v._2._1+1))
			 k+=((v._1,v._2._1))
			}
			else 
			k+=((v._1,max_int))
			}
			).reduceByKey(_ min _)      
            
 }   
            

R.filter( x => x._2!= max_int).sortByKey().collect().foreach(println)
}}										

