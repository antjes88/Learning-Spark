package retailsales

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class EposPipelineBase(spark: SparkSession, edwControlPath: String, dateDimensionPath: String, edwPath: String,
                                skuEDWPath: String)
    extends DataFrameUtilities {
    // path to dimension and fact tables
    val productDimensionPath: String = edwPath + "/product"
    val retailerDimensionPath: String = edwPath + "/retailer"
    val salesFactPath: String = edwPath + "/sales"

    // path to ETL metadata dimension and fact tables
    val batchControlDimensionPath: String = edwControlPath + "/batch"
    val executionDetailedControlFactPath: String = edwControlPath + "/execution_detailed"
    val errorEventControlFactPath: String = edwControlPath + "/error_event"

    // basic dataframes
    val sourceFileDF: DataFrame = readSourceFile()
    val skuMapperDF: DataFrame = spark.read
      .format("parquet")
      .load(skuEDWPath)
    val dateEdwDF: DataFrame = spark.read
      .format("parquet")
      .load(dateDimensionPath)

    // schemas to dimension and fact tables
    val productDimensionSchema: StructType = StructType(List(
        StructField("ProductId", LongType),
        StructField("ProductName", StringType),
        StructField("ProductType", StringType),
        StructField("CreatedTime", TimestampType),
        StructField("CreatedByBatchId", IntegerType), // todo: we need to create the control table
        StructField("UpdatedTime", TimestampType),
        StructField("UpdatedByBatchId", IntegerType), // todo: we need to create the control table
    ))
    val retailerDimensionSchema: StructType = StructType(List(
        StructField("RetailerId", LongType),
        StructField("RetailerName", StringType),
        StructField("CreatedTime", TimestampType),
        StructField("CreatedByBatchId", IntegerType), // todo: we need to create the control table
        StructField("UpdatedTime", TimestampType),
        StructField("UpdatedByBatchId", IntegerType), // todo: we need to create the control table
    ))
    val salesFactSchema: StructType = StructType(List(
        StructField("DateId", LongType),
        StructField("ProductId", LongType),
        StructField("RetailerId", LongType),
        StructField("DataSource", StringType),
        StructField("SKU", IntegerType),
        StructField("UnitsSold", IntegerType),
        StructField("SalesAmount", DecimalType(13,2)),
        StructField("CreatedTime", TimestampType),
        StructField("CreatedByBatchId", IntegerType), // todo: we need to create the control table
        StructField("UpdatedTime", TimestampType),
        StructField("UpdatedByBatchId", IntegerType), // todo: we need to create the control table
    ))

    // todo: schemas to ETL metadata dimension and fact tables
    val batchControlDimensionSchema: StructType = StructType(List(
        StructField("BatchId", LongType),
        StructField("DataSource", StringType),
        StructField("FilePath", StringType),
    ))

    // methods to populate dimension and fact tables, overwritten by child classes
    def readSourceFile(): DataFrame
    def loadProductDimension(): Unit
    def loadRetailerDimension(): Unit
    def loadSalesFact(): Unit

    // todo: methods to populate ETL metadata dimension and fact tables
    def loadControlBatchDimension(filePath: String, dataSource: String): Unit = {
        val batchEtlEdwDF = readTableFromPathOrCreateEmptyDataframeFromSchema(
            spark, batchControlDimensionPath, batchControlDimensionSchema)

        val newMaxId = if (batchEtlEdwDF.count() == 0) 1
            else batchEtlEdwDF.agg(max("BatchId")).head.getInt(0) + 1

        val newBatchDF = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(Row(newMaxId, dataSource, filePath)),
                1),
            batchControlDimensionSchema)

        val toOverwriteBatchEtlEdwDF = batchEtlEdwDF.cache()
          .union(newBatchDF)
          .orderBy("BatchId")

        checkDuplicationOnColumn(toOverwriteBatchEtlEdwDF, "BatchId", "Product Dimension Table")
        List("BatchId", "DataSource", "FilePath")
          .foreach(checkColumnWithoutNulls(toOverwriteBatchEtlEdwDF, _, "Product Dimension Table"))

        toOverwriteBatchEtlEdwDF.write
          .format("parquet")
          .mode("overwrite")
          .save(batchControlDimensionPath)
    }

    // shortcut vies to read metadata
    def viewRetailer(withIds: Boolean = false): DataFrame = {
        val rowRetailer = spark.read
          .format("parquet")
          .load(retailerDimensionPath)
          .orderBy("RetailerName")
          .collect()
          .toList

        if (withIds) spark.createDataFrame(spark.sparkContext.parallelize(rowRetailer,3), retailerDimensionSchema)
          .drop("CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("RetailerName")
        else spark.createDataFrame(spark.sparkContext.parallelize(rowRetailer,3), retailerDimensionSchema)
          .drop("RetailerId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("RetailerName")
    }

    def viewProduct(withIds: Boolean = false): DataFrame = {
        val rowProduct = spark.read
          .format("parquet")
          .load(productDimensionPath)
          .orderBy("ProductName")
          .collect()
          .toList

        if (withIds) spark.createDataFrame(spark.sparkContext.parallelize(rowProduct,3), productDimensionSchema)
          .drop("CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("ProductName")
        else spark.createDataFrame(spark.sparkContext.parallelize(rowProduct,3), productDimensionSchema)
          .drop("ProductId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("ProductName")
    }

    def viewSales(): DataFrame = {
        val rowSales = spark.read
          .format("parquet")
          .load(salesFactPath)
          .collect()
          .toList

        spark.createDataFrame(spark.sparkContext.parallelize(rowSales,3), salesFactSchema)
          .drop("CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .join(
              viewProduct(true).select("ProductName", "ProductId"),
              Seq("ProductId"),
              "inner"
          )
          .join(
              viewRetailer(true).select("RetailerName", "RetailerId"),
              Seq("RetailerId"),
              "inner"
          )
          .join(
              dateEdwDF.select("DateId", "Date"),
              Seq("DateId"),
              "inner"
          )
          .drop("RetailerId", "ProductId", "DateId")
          .select("Date", "ProductName", "RetailerName", "DataSource", "SKU", "UnitsSold", "SalesAmount")
          .orderBy("Date", "ProductName", "RetailerName")
    }
}
