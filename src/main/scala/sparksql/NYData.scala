package sparksql

import org.apache.spark.sql.{Encoders, SparkSession}

case class NYData(
                   id  : BigInt,
                   Case_Number : String,
                   date : java.sql.Date,
                   Block : String,
                   IUCR : Int,
                   Primary_type : String,
                   Description : String,
                   Location_description : String,
                   Arrest : Boolean,
                   Domestic : Boolean,
                   Beat : Int,
                   District : Int,
                   Ward : Int,
                   community : Int,
                   Fbicode : String,
                   X_Co : BigInt,
                   Y_Co : BigInt,
                   Year : Int,
                   Updated_on : String,
                   latitude : Double,
                   longitude : Double,
                   location : String

                 )
//case class sample(id:TimestampType)

object NYData {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().appName("NY Data").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    // val lines = spa.read.csv("data/Crime_dataset").show()

    val data = spark.read.schema(Encoders.product[NYData].schema).option("dateFormat","DD-mm-yyyy").csv("data/crime_dataset")
    data.show()
    data.createOrReplaceTempView("data2017")
    val sample = spark.sql("""
        select * from data2017
      """)
    sample.show()
    //data.createOrReplaceTempView("data")
    /*val an = spa.sql("""
             select * from data;
          """).show()*/

  }

}