name := "sparkPractice"

version := "0.1"

scalaVersion := "2.11.8"



libraryDependencies += "org.scalafx" % "scalafx_2.11" % "8.0.102-R11"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.1"
//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
//libraryDependencies += "org.apache.maven.plugins" % "maven-shade-plugin" % "3.1.0"



