package retailsales

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import retailsales.Functions.{folderCleaner, getSparkAppConf}

import scala.reflect.io.File

class MyEPOSDataProviderPipelineTest extends FunSuite
  with BeforeAndAfterAll with BeforeAndAfter {
    @transient var spark: SparkSession = _
    @transient val productDimensionPath: String = "data_lake/test/edw/retail_sales/product"
    @transient val retailerDimensionPath: String = "data_lake/test/edw/retail_sales/retailer"
    @transient val skuEDWPath: String = "data_lake/edw/etl/sku_mapper"
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
          .config(getSparkAppConf("src/test/data/spark.conf"))
          .getOrCreate()
    }

    override def afterAll(): Unit = {
        spark.stop()
    }

    before {
        folderCleaner(List(productDimensionPath, retailerDimensionPath))
    }

    after {
        folderCleaner(List(productDimensionPath, retailerDimensionPath))
    }

    test("SKU mapper table exists at EDW") {
        assert(File("data_lake/edw/etl/sku_mapper").exists, "Folder should exists")
    }

    test("Read sample file") {
        val spark2 = spark
        import spark2.implicits._

        val pipelineRunner = new MatPipeline(
            spark2,
            "data_lake/edw/etl/sku_mapper",
            "src/test/data/read_sample_file",
            "Dummy",
            "Dummy"
        )

        assert(pipelineRunner.sourceFileDF.count() == 12, "Record number should be 5")

        assert(pipelineRunner.sourceFileDF.schema("SKU").dataType.typeName == "integer",
            "SKU datatype should be integer")
        assert(pipelineRunner.sourceFileDF.schema("Retailer").dataType.typeName == "string",
            "Retailer datatype should be string")
        assert(pipelineRunner.sourceFileDF.schema("Week Ending").dataType.typeName == "date",
            "Week Ending datatype should be date")
        assert(pipelineRunner.sourceFileDF.schema("Sales").dataType.typeName == "float",
            "Sales datatype should be float")
        assert(pipelineRunner.sourceFileDF.schema("Units").dataType.typeName == "integer",
            "Units datatype should be integer")

        assert(pipelineRunner.sourceFileDF
          .select(col("Week Ending").cast("string"))
          .map(_.getString(0))
          .distinct()
          .collect()
          .toList == List("2022-01-02")
            , "All dates for dataframe should be 2022-01-02")
    }

    test("test loadProductDimension"){
        val firstSourceFile: String = "src/test/data/retail_sales_raw/2022-01-02"
        val secondSourceFile: String = "src/test/data/retail_sales_raw/2022-01-09"

        val pipelineRunner = new MatPipeline(
            spark, skuEDWPath, firstSourceFile, productDimensionPath, retailerDimensionPath)
        pipelineRunner.loadProductDimension()
        pipelineRunner.loadRetailerDimension()

        val expectedProductDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(Row(1L, "Coca Cola 330ml Pack 8", "Beverage"),
                    Row(2L, "Coca Cola 2l", "Beverage")),3),
            pipelineRunner.productDimensionSchema)
          .drop("ProductId")
          .orderBy("ProductName")

        val expectedRetailerDF1 = spark.createDataFrame(spark.sparkContext.parallelize(
            List(Row(1L, "Marks"), Row(2L, "Tesco")), 3), pipelineRunner.retailerDimensionSchema)
          .drop("RetailerId")
          .orderBy("RetailerName")

        val rowProduct = spark.read
          .format("parquet")
          .load(productDimensionPath)
          .orderBy("ProductName")
          .collect()
          .toList

        val productDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(rowProduct,3), pipelineRunner.productDimensionSchema)
          .drop("ProductId")
          .orderBy("ProductName")

        val rowRetailer = spark.read
          .format("parquet")
          .load(retailerDimensionPath)
          .orderBy("RetailerName")
          .collect()
          .toList

        val retailerDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(rowRetailer,3), pipelineRunner.retailerDimensionSchema)
          .drop("RetailerId")
          .orderBy("RetailerName")

        val pipelineRunner2 = new MatPipeline(
            spark, skuEDWPath, secondSourceFile, productDimensionPath, retailerDimensionPath)
        pipelineRunner2.loadProductDimension()
        pipelineRunner2.loadRetailerDimension()

        val productDF2 = spark.read
          .format("parquet")
          .load(productDimensionPath)
          .drop("ProductId")
          .orderBy("ProductName")

        val retailerDF2 = spark.read
          .format("parquet")
          .load(retailerDimensionPath)
          .drop("RetailerId")
          .orderBy("RetailerName")

        val expectedProductDF2 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(Row(1L, "Coca Cola 330ml Pack 8", "Beverage"),
                    Row(2L, "Coca Cola 2l", "Beverage"),
                    Row(3L, "Coca Cola 1.25l", "Beverage"),
                    Row(4L, "Warburtons Sliced Tiger Bread 600G", "Bakery")
                ), 3),
            pipelineRunner.productDimensionSchema)
          .drop("ProductId")
          .orderBy("ProductName")

        val expectedRetailerDF2 = spark.createDataFrame(spark.sparkContext.parallelize(
            List(Row(1L, "Marks"), Row(2L, "Tesco"), Row(3L, "Aldi")), 3),
            pipelineRunner.retailerDimensionSchema)
          .drop("RetailerId")
          .orderBy("RetailerName")

        assert(productDF1.except(expectedProductDF1).union(expectedProductDF1.except(productDF1)).count() == 0,
            "After first file population")
        assert(productDF2.except(expectedProductDF2).union(expectedProductDF2.except(productDF2)).count() == 0,
            "After second file population")

        assert(retailerDF1.except(expectedRetailerDF1).union(expectedRetailerDF1.except(retailerDF1)).count() == 0,
            "After first file population")
        assert(retailerDF2.except(expectedRetailerDF2).union(expectedRetailerDF2.except(retailerDF2)).count() == 0,
            "After second file population")
    }
}
