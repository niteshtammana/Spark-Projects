package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object wordCount extends App {

val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
  val ssc = new StreamingContext(conf,Seconds(2))
  ssc.sparkContext.setLogLevel("WARN")
  val lines = ssc.socketTextStream("localhost",9999)
  val words = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey(_+_)
  //window and sliding
  val wordsWindow = words.reduceByKeyAndWindow((a:Int, b:Int) => (a+b),Seconds(10),Seconds(4))
  wordsWindow.print()
  ssc.start()
  ssc.awaitTermination()
}
