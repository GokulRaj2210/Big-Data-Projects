import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Twitter {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(conf)
    val e = sc.textFile(args(0)).map( line => {val a = line.split(",")
		    (a(1).toInt,a(0).toInt) } )
    val q = e.groupByKey().map { case(keyval,valueval)=> var count=0;
                                           for(i <- valueval) {
                                                        count += 1}
                                                (count,1)
                                                 }
        val r = q.reduceByKey((x,y) => x+y)
        r.collect().sorted.foreach(println)
    sc.stop()
  }
}
