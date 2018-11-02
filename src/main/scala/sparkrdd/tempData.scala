package sparkrdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

import scalafx.application.JFXApp

case class tempData(
   day : Int,
   dayOfYear : Int,
   month : Int,
   year : Int,
   prcp : Double,
   snow : Double,
   tave : Double,
   tmax : Double,
   tmin : Double
)
object tempData extends JFXApp {

  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val lines = sc.textFile("Data/MN212142_9392.csv").filter(!_.contains("Day"))
  val data = lines.map{ lines =>
    val rep = lines.replace(",.",",0")
    val p = rep.split(",")
    tempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble  )
  }.persist(StorageLevel.DISK_ONLY)

  val maxTemp = data.reduce((td1,td2) => if (td1.tmax >= td2.tmax) td1 else td2 )

  val maxTemp1 = data.max()(Ordering.by(_.tmax))

  val max = data.map(_.tmax).max()
  val maxReduce = data.filter(x => x.tmax == max)
  //maxReduce.take(5).foreach(println)

  val rainyDays = data.filter(_.prcp >= 1.0).count()
  println(rainyDays)

  val (rainyDaysTemp, rainyCount) = data.aggregate(0.0 -> 0)({case((sum,cnt), td) =>
    if(td.prcp < 1.0) (sum,cnt) else (sum+td.tmax, cnt+1)},{case((a,b),(c,d)) => (a+c,b+d)})
  println(s"rainy days temp is $rainyDaysTemp and rainyCount is $rainyCount")

  val months = data.groupBy(x=> x.month)
  val monthlyTemps = months.map{case (m,days) => (m,days.foldLeft(0.0)((sum,d) => sum + d.tmax/days.size ))}
  val sortedMonths = monthlyTemps.sortBy(x => x._1)
  sortedMonths.foreach(println)

  val yearKeyed = data.map(x => x.year -> x)
  val avgTempByYear = yearKeyed.aggregateByKey(0.0 -> 0)({case((sum,count),td) => (sum+td.tmax,count+1)},
    {case((s1,c1),(s2,c2)) => (s1+s2,c1+c2)})
  println("count: " + avgTempByYear.count())
  avgTempByYear.take(5).foreach(println)

  val avgTemp = avgTempByYear.map(td => td._1 -> (td._2._1/td._2._2,td._2._2))
  val avgTemp1 = avgTempByYear.map{case(y,(a,d)) => (y,(a/d,d))}
  println("count: " + avgTemp1.count())
  avgTemp1.take(5).foreach(println)

  val avgTermPlot = avgTempByYear.collect().sortBy(_._1)
  val longTermPlot = Plot.scatterPlotWithLines(avgTermPlot.map(_._1),avgTermPlot.map { case(a,(s, c)) => (s/c) }, symbolSize = 0, symbolColor = BlackARGB,
    lineGrouping = 1)

  data.unpersist()
  //FXRenderer(longTermPlot, 800, 600)
  sc.stop()
}
