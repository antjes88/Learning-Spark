name := "RetailSales"
version := "0.1"
scalaVersion := "2.12.10"

autoScalaLibrary := false
//val sparkVersion = "3.2.1"
val sparkVersion = "3.0.0"

val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
