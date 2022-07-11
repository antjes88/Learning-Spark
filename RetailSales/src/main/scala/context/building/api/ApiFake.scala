package context.building.api

import context.building.dataframescreator._
import date.DateDimensionBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import retailsales.Functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import retailsales.Orchestrator

object ApiFake extends Serializable {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info("Cleaning Data Lake")
        folderCleaner(List("data_lake/edw", "data_lake/raw")) // comment out if not creating from zero

        logger.info("Parameters definition")
        val sparkAppConfigFile: String = "sparkApiFake.conf"
        val matPath: String = "data_lake/raw/external/mat/retail_sales"
        val skuEDWPath: String = "data_lake/edw/etl/sku_mapper"
        val skuRawPath: String = "data_lake/raw/internal/Marketing/sku_mapper"
        val dateEdwPath: String = "data_lake/edw/retail_sales/date"
        val dateDimensionBuilderFromDate: String = "01/01/2022"
        val dateDimensionBuilderToDate: String = "01/01/2023"
        val fakeDataFromDate: String = "01/01/2022"
        val fakeDataToDate: String = "05/01/2022"

        logger.info("Starting spark session")
        val spark = SparkSession.builder()
          .config(getSparkAppConf(sparkAppConfigFile))
          .getOrCreate()

        logger.info("Creating Date Dimension")
        DateDimensionBuilder.main(
            Array(dateEdwPath, dateDimensionBuilderFromDate, dateDimensionBuilderToDate, sparkAppConfigFile))

        logger.info("Creating SKUMapper")
        DataFramesCreator.SkuMapperBuilder(logger, spark, skuEDWPath, skuRawPath)

        logger.info(s"Creation of all Sundays from $fakeDataFromDate to $fakeDataToDate")
        LocalDate.parse(fakeDataFromDate, DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay
          .until(LocalDate.parse(fakeDataToDate, DateTimeFormatter.ofPattern("dd/MM/yyyy")).toEpochDay)
          .map(LocalDate.ofEpochDay)
          .filter(_.getDayOfWeek.toString == "SUNDAY")
          .foreach(date => {
              DataFramesCreator.matSourceFilesBuilder(logger, spark, date, matPath, skuEDWPath)
              Orchestrator.main(Array(s"$matPath/${date.toString}", skuEDWPath, dateEdwPath))
          })

        logger.info("Finishing spark session")
        spark.stop()
    }
}
