package retailsales

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, current_timestamp, hash, isnull, lit}
import org.apache.spark.sql.types.{DateType, DecimalType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}


class MatEposPipeline(spark: SparkSession, edwPath: String, skuEDWPath: String, dateDimensionPath: String,
                      sourceFilePath: String, edwControlPath: String)
  extends EposPipelineBase(spark, edwControlPath, dateDimensionPath, edwPath, skuEDWPath) {

    override def readSourceFile(): DataFrame = {
        val sampleSchemaStruct = StructType(List(
            StructField("SKU", IntegerType),
            StructField("Retailer", StringType),
            StructField("Week Ending", DateType),
            StructField("Units", FloatType),
            StructField("Sales", FloatType),
        ))

        spark.read
          .format("csv")
          .option("header", "true")
          .option("dateFormat", "d/M/y")
          .schema(sampleSchemaStruct)
          .load(sourceFilePath)
          .withColumn("Units", col("Units").cast(IntegerType))
    }

    override def loadProductDimension(): Unit = {
        val productEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, productDimensionPath, productDimensionSchema)

        val productSourceDF = sourceFileDF
          .join(skuMapperDF, Seq("SKU", "Retailer"), "left")
          .select("Description", "ProductType").distinct()
          .withColumnRenamed("Description", "ProductName")

        val toInsertDF = productSourceDF
          .join(productEdwDF.select("ProductName", "ProductId"), Seq("ProductName"), "left")
          .where(isnull(col("ProductId")))
          .withColumn("ToHash", concat(col("ProductName"), lit(" Product")))
          .withColumn("ProductId", hash(col("ToHash")).cast(LongType) + Int.MaxValue)
          .withColumn("CreatedTime", current_timestamp())
          .withColumn("CreatedByBatchId", lit(null).cast(IntegerType))
          .withColumn("UpdatedTime", lit(null).cast(TimestampType))
          .withColumn("UpdatedByBatchId", lit(null).cast(IntegerType))
          .drop("ToHash")

        val toOverwriteProductEdwDF = productEdwDF.cache()
          .union(toInsertDF
            .select("ProductId", "ProductName", "ProductType", "CreatedTime", "CreatedByBatchId",
                "UpdatedTime", "UpdatedByBatchId"))

        List("ProductId", "ProductName")
          .foreach(checkDuplicationOnColumn(toOverwriteProductEdwDF, _, "Product Dimension Table"))
        List("ProductId", "ProductName", "ProductType")
          .foreach(checkColumnWithoutNulls(toOverwriteProductEdwDF, _, "Product Dimension Table"))

        toOverwriteProductEdwDF.write
          .format("parquet")
          .mode("overwrite")
          .save(productDimensionPath)
    }

    override def loadRetailerDimension(): Unit = {
        val retailerEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, retailerDimensionPath, retailerDimensionSchema)

        val retailerSourceDF = sourceFileDF
          .select("Retailer").distinct()
          .withColumnRenamed("Retailer", "RetailerName")

        val toInsertDF = retailerSourceDF
          .join(retailerEdwDF, Seq("RetailerName"), "left")
          .where(isnull(col("RetailerId")))
          .withColumn("ToHash", concat(col("RetailerName"), lit(" Product")))
          .withColumn("RetailerId", hash(col("ToHash")).cast(LongType) + Int.MaxValue)
          .withColumn("CreatedTime", current_timestamp())
          .withColumn("CreatedByBatchId", lit(null).cast(IntegerType))
          .withColumn("UpdatedTime", lit(null).cast(TimestampType))
          .withColumn("UpdatedByBatchId", lit(null).cast(IntegerType))
          .drop("ToHash")
          .select("RetailerId", "RetailerName", "CreatedTime", "CreatedByBatchId", "UpdatedTime",
              "UpdatedByBatchId")

        val toOverwriteRetailerEdwDF = retailerEdwDF
          .cache()
          .union(toInsertDF)

        List("RetailerId", "RetailerName")
          .foreach(checkDuplicationOnColumn(toOverwriteRetailerEdwDF, _, "Retailer Dimension Table"))
        List("RetailerId", "RetailerName")
          .foreach(checkColumnWithoutNulls(toOverwriteRetailerEdwDF, _, "Retailer Dimension Table"))

        toOverwriteRetailerEdwDF.write
          .format("parquet")
          .mode("overwrite")
          .save(retailerDimensionPath)
    }

    override def loadSalesFact(): Unit = {
        val salesEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, salesFactPath, salesFactSchema)
          .cache()
        val productEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, productDimensionPath, productDimensionSchema)
          .cache()
        val retailerEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, retailerDimensionPath, retailerDimensionSchema)
          .cache()

        val salesSourceDF = sourceFileDF
          .withColumnRenamed("Week Ending", "Date")
          .withColumnRenamed("Units", "UnitsSold")
          .withColumnRenamed("Sales", "SalesAmount")
          .withColumn("SalesAmount", col("SalesAmount").cast(DecimalType(13,2)))
          .withColumn("DataSource", lit("MAT"))

        val salesWithIds = salesSourceDF
          .join(skuMapperDF, Seq("SKU", "Retailer"), "left")
          .withColumnRenamed("Retailer", "RetailerName")
          .withColumnRenamed("Description", "ProductName")
          .join(productEdwDF.select("ProductId", "ProductName"), Seq("ProductName"), "left")
          .join(retailerEdwDF.select("RetailerId", "RetailerName"), Seq("RetailerName"), "left")
          .join(dateEdwDF.select("Date", "DateId"), Seq("Date"), "left")
          .drop("Date", "RetailerName", "ProductName", "ProductType")

        val salesToInsert = salesWithIds
          .join(
              salesEdwDF
                .select("DateId", "ProductId", "RetailerId")
                .withColumn("DummyColumn", lit("DummyValue")),
              Seq("DateId", "ProductId", "RetailerId"),
              "left"
          )
          .where(isnull(col("DummyColumn")))
          .drop("DummyColumn")
          .withColumn("CreatedTime", current_timestamp())
          .withColumn("CreatedByBatchId", lit(null).cast(IntegerType))
          .withColumn("UpdatedTime", lit(null).cast(TimestampType))
          .withColumn("UpdatedByBatchId", lit(null).cast(IntegerType))
          .select("DateId", "ProductId", "RetailerId", "DataSource", "SKU", "UnitsSold", "SalesAmount",
              "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")

        val salesToKeep = salesEdwDF
          .join(
              salesWithIds.withColumn("DummyColumn", lit("DummyValue"))
                .select("DateId", "ProductId", "RetailerId", "DummyColumn"),
              Seq("DateId", "ProductId", "RetailerId"),
              "left"
          )
          .where(isnull(col("DummyColumn")))
          .drop("DummyColumn")
          .select("DateId", "ProductId", "RetailerId", "DataSource", "SKU", "UnitsSold", "SalesAmount",
              "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")

        val salesToUpdate = salesEdwDF
          .join(
              salesWithIds
                .withColumnRenamed("SKU", "SKUUpdated")
                .withColumnRenamed("UnitsSold", "UnitsSoldUpdated")
                .withColumnRenamed("SalesAmount", "SalesAmountUpdated")
                .withColumnRenamed("DataSource", "DataSourceUpdated"),
              Seq("DateId", "ProductId", "RetailerId"),
              "inner"
          )
          .where(
              col("SKU") =!= col("SKUUpdated")
                || col("UnitsSold") =!= col("UnitsSoldUpdated")
                || col("SalesAmount") =!= col("SalesAmountUpdated")
                || col("DataSource") =!= col("DataSourceUpdated")
          )
          .drop("SKU", "UnitsSold", "SalesAmount", "DataSource")
          .withColumnRenamed("SKUUpdated", "SKU")
          .withColumnRenamed("UnitsSoldUpdated", "UnitsSold")
          .withColumnRenamed("SalesAmountUpdated", "SalesAmount")
          .withColumnRenamed("DataSourceUpdated", "DataSource")
          .withColumn("UpdatedTime", current_timestamp())
          .withColumn("UpdatedByBatchId", lit(null).cast(IntegerType))
          .select("DateId", "ProductId", "RetailerId", "DataSource", "SKU", "UnitsSold", "SalesAmount",
              "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")

        val toOverwriteSalesEdwDF = salesToKeep
          .union(salesToInsert)
          .union(salesToUpdate)

        List("DateId", "ProductId", "RetailerId")
          .foreach(checkColumnWithoutNulls(toOverwriteSalesEdwDF, _, "Sales Fact Table"))
        checkDuplicationOnListOfColumns(
            toOverwriteSalesEdwDF, List("DateId", "ProductId", "RetailerId"), "Sales Fact Table")

        toOverwriteSalesEdwDF.write
          .format("parquet")
          .mode("overwrite")
          .save(salesFactPath)
    }
}
