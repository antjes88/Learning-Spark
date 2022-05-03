package date

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, date_format, udf}
import org.apache.spark.sql.types.IntegerType
import retailsales.Functions.getSparkAppConf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date
import scala.annotation.tailrec

object DateDimensionBuilder extends Serializable {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info("Setting parameters") // args is to allow for a test entrypoint
        val dateEdwPath: String = if (args.length > 0) args(0) else "data_lake/edw/retail_sales/date"
        val fromDate: String = if (args.length > 0) args(1) else "01/01/2022"
        val toDate: String = if (args.length > 0) args(2) else "01/01/2023"

        logger.info("Creating spark Session")
        val spark = SparkSession.builder()
          .config(getSparkAppConf("sparkApiFake.conf"))
          .getOrCreate()

        logger.info(s"Creating LocalDate sequence from $fromDate to $toDate")
        val dateSeq = LocalDate
          .parse(fromDate, DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay
          .until(LocalDate.parse(toDate, DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay)
          .map(LocalDate.ofEpochDay)

        logger.info("Creating Date Dimension Dataframe")
        import spark.implicits._
        val dateDimension = dateSeq.toDF("Date")
          .withColumn("DateId", date_format(col("Date"), "yyyyMMdd"))
          .withColumn("WeekEnding", weekEndingFinderUDF(col("Date")))
          .withColumn("WeekCommencing", weekCommencingFinderUDF(col("Date")))
          .withColumn("DayOfWeekName", date_format(col("Date"), "EEEE"))
          .withColumn("DayOfWeekNumber", dayOfWeekNumberFinderUDF(col("Date")))
          .withColumn("DayOfMonth", date_format(col("Date"), "d").cast(IntegerType))
          .withColumn("DayOfYear", date_format(col("Date"), "D").cast(IntegerType))
          .withColumn("MonthName", date_format(col("Date"), "MMMM"))
          .withColumn("MonthNumber", date_format(col("Date"), "M").cast(IntegerType))
          .withColumn("QuarterNumber", date_format(col("Date"), "q").cast(IntegerType))
          .withColumn("QuarterPresentation", date_format(col("Date"), "QQQ"))
          .withColumn("Year", date_format(col("Date"), "yyyy").cast(IntegerType))
          .select("DateId", "Date", "WeekEnding", "WeekCommencing", "DayOfWeekName", "DayOfWeekNumber",
              "DayOfMonth", "DayOfYear", "MonthName", "MonthNumber", "QuarterNumber", "QuarterPresentation", "Year")

        logger.info(s"Writing Date Dimension to $dateEdwPath")
        dateDimension.write
          .format("parquet")
          .mode("overwrite")
          .save(dateEdwPath)

        logger.info("Finishing Spark Session")
        spark.stop()
    }

    def weekEndingFinder(myDate: Date): Date = {
        @tailrec
        def accumulator(myDate: LocalDate): Date = {
            if (myDate.getDayOfWeek.toString == "SUNDAY") Date.valueOf(myDate)
            else accumulator(myDate.plusDays(1))
        }

        accumulator(myDate.toLocalDate)
    }

    def weekEndingFinderUDF: UserDefinedFunction = udf(weekEndingFinder(_: Date): Date)

    def weekCommencingFinder(myDate: Date): Date = {
        @tailrec
        def accumulator(myDate: LocalDate): Date = {
            if (myDate.getDayOfWeek.toString == "MONDAY") Date.valueOf(myDate)
            else accumulator(myDate.plusDays(-1))
        }

        accumulator(myDate.toLocalDate)
    }

    def weekCommencingFinderUDF: UserDefinedFunction = udf(weekCommencingFinder(_: Date): Date)

    def dayOfWeekNumberFinder(myDate: Date): Integer = {
        myDate.toLocalDate.getDayOfWeek.toString match {
            case "MONDAY"    => 1
            case "TUESDAY"   => 2
            case "WEDNESDAY" => 3
            case "THURSDAY"  => 4
            case "FRIDAY"    => 5
            case "SATURDAY"  => 6
            case "SUNDAY"    => 7
            case _ => throw new Exception("dayOfWeekFinder returned an unexpected case.")
        }
    }

    def dayOfWeekNumberFinderUDF: UserDefinedFunction = udf(dayOfWeekNumberFinder(_: Date))
}
