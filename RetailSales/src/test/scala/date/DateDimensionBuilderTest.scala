package date

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, LongType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import retailsales.Functions.getSparkAppConf

import java.sql.Date

class DateDimensionBuilderTest extends FunSuite with BeforeAndAfterAll {
    @transient var spark: SparkSession = _
    @transient var sparkAppConfigFile: String = "src/test/data/spark.conf"

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
          .config(getSparkAppConf(sparkAppConfigFile))
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


    test("test DateDimensionBuilder.main() general") {
        val testDatePath = "data_lake/test/date_testing"
        DateDimensionBuilder.main(
            Array(testDatePath, "01/01/2022", "08/01/2022", sparkAppConfigFile))

        val resultDateDF = spark.read
          .format("parquet")
          .load(testDatePath)
          .sort("DateId")

        val dateSchema = StructType(List(
            StructField("DateId", LongType),
            StructField("Date", StringType),
            StructField("WeekEnding", StringType),
            StructField("WeekCommencing", StringType),
            StructField("DayOfWeekName", StringType),
            StructField("DayOfWeekNumber", IntegerType),
            StructField("DayOfMonth", IntegerType),
            StructField("DayOfYear", IntegerType),
            StructField("MonthName", StringType),
            StructField("MonthNumber", IntegerType),
            StructField("QuarterNumber", IntegerType),
            StructField("QuarterPresentation", StringType),
            StructField("Year", IntegerType),
        ))

        val myRows = List(
            Row(20220101L, "01/01/2022", "02/01/2022", "27/12/2021", "Saturday", 6, 1, 1, "January", 1, 1, "Q1", 2022),
            Row(20220102L, "02/01/2022", "02/01/2022", "27/12/2021", "Sunday", 7, 2, 2, "January", 1, 1, "Q1", 2022),
            Row(20220103L, "03/01/2022", "09/01/2022", "03/01/2022", "Monday", 1, 3, 3, "January", 1, 1, "Q1", 2022),
            Row(20220104L, "04/01/2022", "09/01/2022", "03/01/2022", "Tuesday", 2, 4, 4, "January", 1, 1, "Q1", 2022),
            Row(20220105L, "05/01/2022", "09/01/2022", "03/01/2022", "Wednesday", 3, 5, 5, "January", 1, 1, "Q1", 2022),
            Row(20220106L, "06/01/2022", "09/01/2022", "03/01/2022", "Thursday", 4, 6, 6, "January", 1, 1, "Q1", 2022),
            Row(20220107L, "07/01/2022", "09/01/2022", "03/01/2022", "Friday", 5, 7, 7, "January", 1, 1, "Q1", 2022)
        )
        val myRDD = spark.sparkContext.parallelize(myRows, 3)
        val expectedDateDF = spark.createDataFrame(myRDD, dateSchema)
          .withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
          .withColumn("WeekEnding", to_date(col("WeekEnding"), "dd/MM/yyyy"))
          .withColumn("WeekCommencing", to_date(col("WeekCommencing"), "dd/MM/yyyy"))

        assert(resultDateDF.except(expectedDateDF).union(expectedDateDF.except(resultDateDF)).count() == 0,
            "Day of Week Number calculated should be the ones on expected")
        assert(!resultDateDF.schema.sortBy(_.name).zip(expectedDateDF.schema.sortBy(_.name)).exists(x => x._1 != x._2),
            "Looking for columns that are not the same on the dataframes schema")
    }
}
