package retailsales

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, hash, isnull, lit}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType}


class MatPipeline(spark: SparkSession, skuEDWPath: String, sourceFilePath: String, productDimensionPath: String,
                  retailerDimensionPath: String)
  extends PipelineBase(spark, skuEDWPath)
    with DataFrameUtilities {

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

        val productSourceDF = sourceFileDF.join(skuMapperDF, Seq("SKU", "Retailer"), "inner")
          .select("Description", "ProductType").distinct()
          .withColumnRenamed("Description", "ProductName")

        val toInsertDF = productSourceDF
          .join(productEdwDF.select("ProductName", "ProductId"), Seq("ProductName"), "left")
          .where(isnull(col("ProductId")))
          .withColumn("ToHash", concat(col("ProductName"), lit(" Product")))
          .withColumn("ProductId", hash(col("ToHash")).cast(LongType) + Int.MaxValue)
          .drop("ToHash")

        val toOverwriteProductEdwDF = productEdwDF.cache()
          .union(toInsertDF.select("ProductId", "ProductName", "ProductType"))

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
          .drop("ToHash")

        val toOverwriteRetailerEdwDF = retailerEdwDF.cache()
          .union(toInsertDF.select("RetailerId", "RetailerName"))

        List("RetailerId", "RetailerName")
          .foreach(checkDuplicationOnColumn(toOverwriteRetailerEdwDF, _, "Retailer Dimension Table"))
        List("RetailerId", "RetailerName")
          .foreach(checkColumnWithoutNulls(toOverwriteRetailerEdwDF, _, "Retailer Dimension Table"))

        toOverwriteRetailerEdwDF.write
          .format("parquet")
          .mode("overwrite")
          .save(retailerDimensionPath)
    }
}
