package retailsales

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import retailsales.Functions.{folderCleaner, getSparkAppConf}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.reflect.io.File

class MatEposPipelineTest extends FunSuite
  with BeforeAndAfterAll with BeforeAndAfter {
    @transient var spark: SparkSession = _
    @transient val edwPath: String = "data_lake/test/edw/retail_sales"
    @transient val edwControlPath: String = "data_lake/test/edw/control/retail_sales"
    @transient val dateDimensionPath: String = "data_lake/test/edw/permanent/date"
    @transient val skuEDWPath: String = "data_lake/edw/etl/sku_mapper"
    @transient val salesExpectedSchema: StructType = StructType(List(
        StructField("Date", StringType),
        StructField("ProductName", StringType),
        StructField("RetailerName", StringType),
        StructField("DataSource", StringType),
        StructField("SKU", IntegerType),
        StructField("UnitsSold", IntegerType),
        StructField("SalesAmount", DoubleType),
    ))

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
          .config(getSparkAppConf("src/test/data/spark.conf"))
          .getOrCreate()
    }

    override def afterAll(): Unit = {
        spark.stop()
    }

    before {
        folderCleaner(List(edwPath))
    }

    after {
        folderCleaner(List(edwPath))
    }

    test("SKU mapper table exists at EDW") {
        assert(File("data_lake/edw/etl/sku_mapper").exists, "Folder and data should exists")
    }

    test("Read sample file") {
        val spark2 = spark
        import spark2.implicits._

        val testFile: String = "src/test/data/read_sample_file"
        val pipelineRunner = new MatEposPipeline(
            spark2, edwPath, skuEDWPath, dateDimensionPath, testFile, edwControlPath)

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

    test("test pipeline execution - tables population"){
        val firstSourceFile: String = "src/test/data/retail_sales_raw/2022-01-02"
        val secondSourceFile: String = "src/test/data/retail_sales_raw/2022-01-09"

        val pipelineRunner = new MatEposPipeline(
            spark, edwPath, skuEDWPath, dateDimensionPath, firstSourceFile, edwControlPath)

        pipelineRunner.loadProductDimension()
        pipelineRunner.loadRetailerDimension()
        pipelineRunner.loadSalesFact()
        val productDF1 = pipelineRunner.viewProduct()
        val retailerDF1 = pipelineRunner.viewRetailer()
        val salesDF1 = pipelineRunner.viewSales()

        val expectedProductDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(
                    Row(1L, "Coca Cola 330ml Pack 8", "Beverage", null, null, null, null),
                    Row(2L, "Coca Cola 2l", "Beverage", null, null, null, null)),
                3),
            pipelineRunner.productDimensionSchema)
          .drop("ProductId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("ProductName")

        val expectedRetailerDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(
                    Row(1L, "Marks", null, null, null, null),
                    Row(2L, "Tesco", null, null, null, null)),
                3),
            pipelineRunner.retailerDimensionSchema)
          .drop("RetailerId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("RetailerName")

        val expectedSalesDF1 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(
                    Row("2022-01-02", "Coca Cola 2l", "Tesco", "MAT", 2001, 700, 1979.02),
                    Row("2022-01-02", "Coca Cola 330ml Pack 8", "Marks", "MAT", 1002, 600,979.02)),
                3),
            salesExpectedSchema)
          .withColumn("Date", to_date(col("Date"),"yyyy-MM-dd"))
          .withColumn("SalesAmount", col("SalesAmount").cast(DecimalType(13,2)))
          .orderBy("Date", "ProductName", "RetailerName")

        val pipelineRunner2 = new MatEposPipeline(
            spark, edwPath, skuEDWPath, dateDimensionPath, secondSourceFile, edwControlPath)

        pipelineRunner2.loadProductDimension()
        pipelineRunner2.loadRetailerDimension()
        pipelineRunner2.loadSalesFact()
        val productDF2 = pipelineRunner2.viewProduct()
        val retailerDF2 = pipelineRunner2.viewRetailer()
        val salesDF2 = pipelineRunner2.viewSales()

        val expectedProductDF2 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(
                    Row(1L, "Coca Cola 330ml Pack 8", "Beverage", null, null, null, null),
                    Row(2L, "Coca Cola 2l", "Beverage", null, null, null, null),
                    Row(3L, "Coca Cola 1.25l", "Beverage", null, null, null, null),
                    Row(4L, "Warburtons Sliced Tiger Bread 600G", "Bakery", null, null, null, null)
                ),
                3),
            pipelineRunner.productDimensionSchema)
          .drop("ProductId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("ProductName")

        val expectedRetailerDF2 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(Row(1L, "Marks", null, null, null, null),
                    Row(2L, "Tesco", null, null, null, null),
                    Row(3L, "Aldi", null, null, null, null)),
                3),
            pipelineRunner.retailerDimensionSchema)
          .drop("RetailerId", "CreatedTime", "CreatedByBatchId", "UpdatedTime", "UpdatedByBatchId")
          .orderBy("RetailerName")

        val expectedSalesDF2 = spark.createDataFrame(
            spark.sparkContext.parallelize(
                List(
                    Row("2022-01-02", "Coca Cola 2l", "Tesco", "MAT", 2001, 700, 1979.02),
                    Row("2022-01-02", "Coca Cola 330ml Pack 8", "Marks", "MAT", 1002, 600,979.02),
                    Row("2022-01-09", "Coca Cola 1.25l", "Aldi", "MAT", 3004, 100, 279.02),
                    Row("2022-01-09", "Coca Cola 2l", "Tesco", "MAT", 2001, 700, 1979.02),
                    Row("2022-01-09", "Coca Cola 330ml Pack 8", "Marks", "MAT", 1002, 600, 979.02),
                    Row("2022-01-09", "Warburtons Sliced Tiger Bread 600G", "Aldi", "MAT", 3005, 255, 267.98)
                ),
                3),
            salesExpectedSchema)
          .withColumn("Date", to_date(col("Date"),"yyyy-MM-dd"))
          .withColumn("SalesAmount", col("SalesAmount").cast(DecimalType(13,2)))
          .orderBy("Date", "ProductName", "RetailerName")

        assert(productDF1.except(expectedProductDF1).union(expectedProductDF1.except(productDF1)).count() == 0,
            "Product after first file population")
        assert(productDF2.except(expectedProductDF2).union(expectedProductDF2.except(productDF2)).count() == 0,
            "Product after second file population")

        assert(retailerDF1.except(expectedRetailerDF1).union(expectedRetailerDF1.except(retailerDF1)).count() == 0,
            "Retailer after first file population")
        assert(retailerDF2.except(expectedRetailerDF2).union(expectedRetailerDF2.except(retailerDF2)).count() == 0,
            "Retailer after second file population")

        assert(salesDF1.except(expectedSalesDF1).union(expectedSalesDF1.except(salesDF1)).count() == 0,
            "Sales after first file population")
        assert(salesDF2.except(expectedSalesDF2).union(expectedSalesDF2.except(salesDF2)).count() == 0,
            "Sales after second file population")
    }
}
