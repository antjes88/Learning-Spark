package retailsales

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class PipelineBase (spark: SparkSession, skuEDWPath: String){
    val sourceFileDF: DataFrame = readSourceFile()
    val skuMapperDF: DataFrame = spark.read
      .format("parquet")
      .load(skuEDWPath)
    val productDimensionSchema: StructType = StructType(List(
        StructField("ProductId", LongType),
        StructField("ProductName", StringType),
        StructField("ProductType", StringType)
    ))
    val retailerDimensionSchema: StructType = StructType(List(
        StructField("RetailerId", LongType),
        StructField("RetailerName", StringType),
    ))

    def readSourceFile(): DataFrame
    def loadProductDimension(): Unit
    def loadRetailerDimension(): Unit
//    def loadSalesFact(): Unit
}
