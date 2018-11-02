package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.functions._

import scalafx.application.JFXApp


object NOAAData extends JFXApp {


  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val tschema = StructType(Array(
    StructField("sid", StringType),
    StructField("date", DateType),
    StructField("mtype", StringType),
    StructField("value", DoubleType)
  ))

  val sschema = StructType(Array(
    StructField("sid", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("name", StringType)
  ))

  val stationRDD = spark.sparkContext.textFile("Data/ghcnd-stations.txt").map { lines =>
    val sid = lines.substring(0, 11)
    val lat = lines.substring(12, 20).toDouble
    val lon = lines.substring(22, 30).toDouble
    val name = lines.substring(41, 71)
    Row(sid, lat, lon, name)
  }
  val station = spark.createDataFrame(stationRDD, sschema).cache()
  //station.show()
  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyymmdd").csv("Data/2017.csv").cache()
  //data2017.show()
  //data2017.schema.printTreeString()

  data2017.createOrReplaceTempView("data2017")

  /* val sqlData2017 = spark.sql("""
       select sid,date,(tmax + tmin)/20*1.8 + 32 as tave from
       (select sid,date,value as tmax from data2017 where mtype = "TMAX" limit 1000)
       join
       (select sid,date,value as tmin from data2017 where mtype = "TMIN" limit 1000)
       using (sid,date)
     """)

   sqlData2017.show()*/

  val tmax = data2017.filter($"mtype" === "TMAX").drop('mtype).withColumnRenamed("value", "tmax").limit(1000000)
  //tmax.describe().show()
  val tmin = data2017.filter('mtype === "TMIN").drop('mtype).withColumnRenamed("value", "tmin").limit(1000000)
  //val combined2017 = tmax.join(tmin, tmax("sid") === tmin("sid") && tmax("date") === tmin("date"))
  val joined2017 = tmax.join(tmin, Seq("sid", "date"))
  val select2017 = joined2017.select('sid, 'date, ('tmax + 'tmin) / 20 * 1.8 + 32).withColumnRenamed("((((tmax + tmin) / 20) * 1.8) + 32)", "tave")

  val stationGroupAvg = select2017.groupBy('sid).agg(avg('tave))
  //stationGroupAvg.show()
  val comb = stationGroupAvg.join(station, "sid")
  //val comb1 = stationGroupAvg.join(station, stationGroupAvg("sid") === station("sid"))

  val totalData = comb.collect()
  val temps = totalData.map(x => x.getDouble(1))
  val lats = totalData.map(_.getDouble(2))
  val longs = totalData.map(_.getDouble(3))
  val cg = ColorGradient(0.0 -> BlueARGB, 50.0 -> GreenARGB, 100.0 -> RedARGB)
  val plot = Plot.scatterPlot(longs, lats, title = "Global Temps", xLabel = "Longitude",
    yLabel = "Latitude", symbolSize = 3, symbolColor = temps.map(cg))
  FXRenderer(plot, 800, 600)

  spark.stop()


}
