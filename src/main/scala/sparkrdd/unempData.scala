package sparkrdd

import org.apache.spark.{SparkConf, SparkContext}

case class laData(area:String,text:String)
case class laSeries(id:String,area:String,measure:Int,title:String)
case class laMin(series:String,year:Int,period:Int,value:Double)

object unempData extends App {

  val config = new SparkConf().setMaster("local[*]").setAppName("Unemployement Data")
  val sc = new SparkContext(config)

  sc.setLogLevel("WARN")

  val data = sc.textFile("Data/la.area.txt").filter(!_.contains("area_code")).map { lines =>
    val p = lines.split("\t").map(_.trim)
    laData(p(1),p(2))
  }.cache()

  val series = sc.textFile("Data/la.series.txt").filter(!_.contains("area_code")).map{lines =>
    val p = lines.split("\t").map(_.trim)
    laSeries(p(0),p(2),p(3).toInt,p(6))
  }.cache()

  val minData = sc.textFile("Data/la.data.30.Minnesota.txt").filter(!_.contains("year")).map{lines =>
    val p = lines.split("\t").map(_.trim)
    laMin(p(0),p(1).toInt,p(2).drop(1).toInt,p(3).toDouble)
  }.cache()

  //data.take(5) foreach(println)
  //series.take(5) foreach(println)
  minData.take(5) foreach(println)

  val rates = minData.filter(_.series.endsWith("03"))
  val decades = rates.map(x => (x.series,x.year/10) -> x.value)
  val decadeAve = decades.aggregateByKey(0.0 -> 0)({case ((s,c),d) => (s+d,c+1)},{case((s1,c1),(s2,c2)) => (s1+s2,c1+c2)}).
    mapValues(t => t._1/t._2)
  val maxDecadeAve = decadeAve.map{case((id,dec),avg) => (id -> (dec,avg))}.reduceByKey{ case ((d1,a1),(d2,a2)) =>
  if(a1>=a2) (d1,a1) else (d2,a2)}
    //filter(x => x._2._1.equals(199))
  val seriesData = series.map(x => x.id -> x.title)
  val joinedData = seriesData.join(maxDecadeAve)

  val dataByArea = joinedData.mapValues{case(a,(b,c)) => (a,b,c)}.map{ case(x,t) => (x.drop(3).dropRight(2) -> t)}

  val area = data.map(x => x.area -> x.text)

  val fullJoinedTable = area.join(dataByArea)

  fullJoinedTable
    .take(5).foreach(println)


  sc.stop()
}
