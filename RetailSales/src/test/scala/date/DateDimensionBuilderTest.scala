package date

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import retailsales.Functions.getSparkAppConf

import java.sql.Date

class DateDimensionBuilderTest extends FunSuite with BeforeAndAfterAll {
    @transient var spark: SparkSession = _

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
          .config(getSparkAppConf("src/test/data/spark.conf"))
          .getOrCreate()
    }

    override def afterAll(): Unit = {
        spark.stop()
    }

    def dateDfFromList(myList: List[String], colName: String): DataFrame = {
        val myRows = myList.map(myDate => Row(Date.valueOf(myDate)))
        val myRDD = spark.sparkContext.parallelize(myRows, 3)

        spark.createDataFrame(myRDD, StructType(List(StructField(colName, DateType))))
    }

    test("test WeekEndingFinder") {
        val datesToTest = List("2018-01-05", "2022-01-01", "2022-01-02", "1983-12-31", "2027-03-14")
          .map(Date.valueOf)
        val expected = List("2018-01-07", "2022-01-02", "2022-01-02", "1984-01-01", "2027-03-14")

        assert(datesToTest.map(date.DateDimensionBuilder.weekEndingFinder(_).toString) == expected,
            "Week Ending calculated should be the ones on expected")
    }

    test("test WeekEndingFinderUDF") {
        val dateDF = dateDfFromList(
            List("2018-01-05", "2022-01-01", "2022-01-02", "1983-12-31", "2027-03-14"), "Date")
        val calculatedDF = dateDF
          .withColumn("WeekEnding", date.DateDimensionBuilder.weekEndingFinderUDF(col("Date")))
          .drop("Date")

        val expectedDF = dateDfFromList(
            List("2018-01-07", "2022-01-02", "2022-01-02", "1984-01-01", "2027-03-14"), "WeekEnding")

        assert(calculatedDF.except(expectedDF).union(expectedDF.except(calculatedDF)).count() == 0,
            "Week Ending calculated should be the ones on expected")
    }

    test("test weekCommencingFinder") {
        val datesToTest = List("2018-01-05", "2022-01-01", "2022-01-02", "1983-12-31", "2027-03-14")
          .map(Date.valueOf)
        val expected = List("2018-01-01", "2021-12-27", "2021-12-27", "1983-12-26", "2027-03-08")

        assert(datesToTest.map(date.DateDimensionBuilder.weekCommencingFinder(_).toString) == expected,
            "Week Commencing calculated should be the ones on expected")
    }

    test("test weekCommencingFinderUDF") {
        val dateDF = dateDfFromList(
            List("2018-01-05", "2022-01-01", "2022-01-02", "1983-12-31", "2027-03-14"), "Date")
        val calculatedDF = dateDF
          .withColumn("WeekCommencing", date.DateDimensionBuilder.weekCommencingFinderUDF(col("Date")))
          .drop("Date")

        val expectedDF = dateDfFromList(
            List("2018-01-01", "2021-12-27", "2021-12-27", "1983-12-26", "2027-03-08"), "WeekCommencing")

        assert(calculatedDF.except(expectedDF).union(expectedDF.except(calculatedDF)).count() == 0,
            "Week Commencing calculated should be the ones on expected")
    }

    test("test dayOfWeekNumberFinder") {
        val datesToTest = List(
            "2022-05-02", "2022-05-03", "2022-05-04", "2022-05-05", "2022-05-06", "2022-05-07", "2022-05-08")
          .map(Date.valueOf)
        val expected = 1.to(7).toList

        assert(datesToTest.map(date.DateDimensionBuilder.dayOfWeekNumberFinder) == expected,
            "Day of week number calculated should be the ones on expected")
    }

    test("test dayOfWeekNumberFinderUDF") {
        val dateDF = dateDfFromList(
            List("2022-05-02", "2022-05-03", "2022-05-04", "2022-05-05", "2022-05-06", "2022-05-07", "2022-05-08"),
            "Date"
        )
        val calculatedDF = dateDF
          .withColumn("DayOfWeekNumber",
              date.DateDimensionBuilder.dayOfWeekNumberFinderUDF(col("Date"))
          )
          .drop("Date")

        val expectedDF = spark.createDataFrame(
            spark.sparkContext.parallelize(1.to(7).toList.map(Row(_)), 3),
            StructType(List(StructField("DayOfWeekNumber", IntegerType)))
        )

        assert(calculatedDF.except(expectedDF).union(expectedDF.except(calculatedDF)).count() == 0,
            "Day of Week Number calculated should be the ones on expected")
    }

//    todo: test for creation of date dimenison
}
