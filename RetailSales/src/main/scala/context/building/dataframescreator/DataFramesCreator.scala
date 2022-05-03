package context.building.dataframescreator

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object DataFramesCreator {
    def SkuMapperBuilder(logger: Logger, spark: SparkSession, skuEDWPath: String, skuRawPath: String): Unit = {
        val SKUMapperSchema = StructType(List(
            StructField("SKU", IntegerType),
            StructField("Retailer", StringType),
            StructField("Description", StringType),
            StructField("ProductType", StringType),
        ))
        val myRows = List(
            Row(1001, "Marks", "Coca Cola 2l", "Beverage"),
            Row(2001, "Tesco", "Coca Cola 2l", "Beverage"),
            Row(3001, "Aldi", "Coca Cola 2l", "Beverage"),

            Row(1002, "Marks", "Coca Cola 330ml Pack 8", "Beverage"),
            Row(2002, "Tesco", "Coca Cola 330ml Pack 8", "Beverage"),
            Row(3002, "Aldi", "Coca Cola 330ml Pack 8", "Beverage"),

            Row(1003, "Marks", "Coca Cola 330ml", "Beverage"),
            Row(2003, "Tesco", "Coca Cola 330ml", "Beverage"),
            Row(3003, "Aldi", "Coca Cola 330ml", "Beverage"),

            Row(1004, "Marks", "Coca Cola 1.25l", "Beverage"),
            Row(2004, "Tesco", "Coca Cola 1.25l", "Beverage"),
            Row(3004, "Aldi", "Coca Cola 1.25l", "Beverage"),

            Row(1005, "Marks", "Warburtons Sliced Tiger Bread 600G", "Bakery"),
            Row(2005, "Tesco", "Warburtons Sliced Tiger Bread 600G", "Bakery"),
            Row(3005, "Aldi", "Warburtons Sliced Tiger Bread 600G", "Bakery"),
        )
        val SKUMapperRDD = spark.sparkContext.parallelize(myRows, 3)
        val SKUMapperDF = spark.createDataFrame(SKUMapperRDD, SKUMapperSchema)

        // todo: check that the SKU are unique and the 1,2,3 are respectively for Marks, Tesco, Aldi

        logger.info("Saving to disk SKUMapper")
        SKUMapperDF.write
          .format("parquet")
          .mode("overwrite")
          .save(skuRawPath)

        SKUMapperDF.write
          .format("parquet")
          .mode("overwrite")
          .save(skuEDWPath)
    }

    def matSourceFilesBuilder(logger: Logger, spark: SparkSession, date: LocalDate, matPath: String,
                              skuEDWPath: String): Unit = {
        logger.info(s"Mat source file created is for date: ${date.toString}")
        val SKUMapperDF = spark.read
          .format("parquet")
          .load(skuEDWPath)
          .where(col("Retailer").isin("Marks", "Tesco", "Aldi"))

        val matSourceDF = SKUMapperDF
          .withColumn("Week Ending", lit(date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))))
          .withColumn("UnitaryPrice", rand() * 3)
          .withColumn("Units", round(rand() * 1000))
          .withColumn("Sales", round(col("UnitaryPrice") * col("Units"), 2))
          .drop("Description", "UnitaryPrice", "ProductType")
          .sort(col("Retailer"), col("SKU"))

        logger.info("Saving to disk Mat Source File")
        matSourceDF.repartition(1).write
          .format("csv")
          .option("header", "true")
          .mode("overwrite")
          .save(s"$matPath/${date.toString}")
    }
}
