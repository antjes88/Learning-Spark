package retailsales

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import retailsales.Functions.getSparkAppConf

case class MySchema(Id: Integer, Name: String)

class DataFrameUtilitiesTest extends FunSuite with BeforeAndAfterAll {
    @transient var spark: SparkSession = _
    @transient var SKUMapperSchema: StructType = _
    @transient var listOfColumnsSchema: StructType = _
    @transient var myTraitUnderTest: DataFrameUtilities = new DataFrameUtilities {}

    override def beforeAll(): Unit = {
        spark = SparkSession.builder()
          .config(getSparkAppConf("src/test/data/spark.conf"))
          .getOrCreate()

        SKUMapperSchema = StructType(List(
            StructField("SKU", IntegerType),
            StructField("Retailer", StringType)
        ))

        listOfColumnsSchema = StructType(List(
            StructField("Name", StringType),
            StructField("DateId", IntegerType),
            StructField("Company", StringType)
        ))
    }

    override def afterAll(): Unit = {
        spark.stop()
    }


    test("test duplicationOnColumnChecker for DF with duplicates") {
        val myRowsDuplicated = List(Row(1, "Marks"), Row(2, "Tesco"), Row(3, "Marks"))
        val myRDDWithDuplicates = spark.sparkContext.parallelize(myRowsDuplicated, 3)
        val myDFWithDuplicates = spark.createDataFrame(myRDDWithDuplicates, SKUMapperSchema)

        val thrown = intercept[Exception] {
            myTraitUnderTest
              .checkDuplicationOnColumn(myDFWithDuplicates, "Retailer", "myTable")
        }
        assert(thrown.getMessage === "ETL: myTable - Retailer must NOT have duplicates", "Marks is duplicated")
    }


    test("test duplicationOnColumnChecker for DF with NO duplicates") {
        val myRowsNoDuplicates = List(Row(1, "Marks"), Row(2, "Tesco"))
        val myRDDNoDuplicates = spark.sparkContext.parallelize(myRowsNoDuplicates, 3)
        val myDFNoDuplicates = spark.createDataFrame(myRDDNoDuplicates, SKUMapperSchema)

        assert(
            myTraitUnderTest
              .checkDuplicationOnColumn(myDFNoDuplicates, "Retailer", "myTable") === (),
            "DF has no duplicates")
    }


    test("test checkDuplicationOnListOfColumns for DF with duplicates") {
        val myRowsDuplicated = List(Row("Test1", 1, "Marks"), Row("Test1", 2, "Marks"), Row("Test1", 1, "Marks"))
        val myRDDWithDuplicates = spark.sparkContext.parallelize(myRowsDuplicated, 3)
        val myDFWithDuplicates = spark.createDataFrame(myRDDWithDuplicates, listOfColumnsSchema)

        val thrown = intercept[Exception] {
            myTraitUnderTest
              .checkDuplicationOnListOfColumns(
                  myDFWithDuplicates, List("Name", "DateId", "Company"), "myTable")
        }
        assert(thrown.getMessage === "ETL: myTable - List(Name, DateId, Company) must NOT have duplicates",
            "Row(Test 1, 1, Marks_ is duplicated")
    }


    test("test checkDuplicationOnListOfColumns for DF with NO duplicates") {
        val myRowsNoDuplicates = List(Row("Test1", 1, "Marks"), Row("Test1", 2, "Marks"), Row("Test1", 3, "Marks"))
        val myRDDNoDuplicates = spark.sparkContext.parallelize(myRowsNoDuplicates, 3)
        val myDFNoDuplicates = spark.createDataFrame(myRDDNoDuplicates, listOfColumnsSchema)

        assert(
            myTraitUnderTest
              .checkDuplicationOnListOfColumns(
                  myDFNoDuplicates, List("Name", "DateId", "Company"), "myTable") === (),
            "DF has no duplicates")
    }


    test("test checkColumnWithoutNulls for DF with nulls") {
        val myRowsNulls = List(Row(1, "Marks"), Row(2, null), Row(3, "Marks"))
        val myRDDWithNulls = spark.sparkContext.parallelize(myRowsNulls, 3)
        val myDFWithNulls = spark.createDataFrame(myRDDWithNulls, SKUMapperSchema)

        val thrown = intercept[Exception] {
            myTraitUnderTest
              .checkColumnWithoutNulls(myDFWithNulls, "Retailer", "myTable")
        }
        assert(thrown.getMessage === "ETL: myTable - Retailer must NOT have null values", "There is a null value")
    }


    test("test checkColumnWithoutNulls NO nulls") {
        val myRows = List(Row(1, "Marks"), Row(2, "Tesco"))
        val myRDD = spark.sparkContext.parallelize(myRows, 2)
        val myDF = spark.createDataFrame(myRDD, SKUMapperSchema)

        assert(
            myTraitUnderTest
              .checkColumnWithoutNulls(myDF, "Retailer", "myTable") === (),
            "DF has no nulls")
    }


    test("test getMaxFromDataframeColumn for a given table") {
        val myRows = List(Row(1, "Marks"), Row(2, "Tesco"), Row(3, "Marks"))
        val myRDD = spark.sparkContext.parallelize(myRows, 3)
        val myDF = spark.createDataFrame(myRDD, SKUMapperSchema)

        assert(myTraitUnderTest
          .getMaxFromDataframeColumnOrZeroIfEmptyTable(myDF, "SKU") == 3,
            "Max SKU is 3")
    }


    test("test getMaxFromDataframeColumn for an empty table") {
        val spark2: SparkSession = spark
        import spark2.implicits._

        val emptyDF = Seq.empty[MySchema].toDF()

        assert(myTraitUnderTest
          .getMaxFromDataframeColumnOrZeroIfEmptyTable(emptyDF, "Id") == 0,
            "Max Id is 0 as the df is empty")
    }


    test("test readTableFromPathOrCreateEmptyDataframeFromSchema for a given table") {
        val myRows = List(Row(1, "Marks"), Row(2, "Tesco"), Row(3, "Marks"))
        val myRDD = spark.sparkContext.parallelize(myRows, 3)
        val myDF = spark.createDataFrame(myRDD, SKUMapperSchema)

        assert(myTraitUnderTest
          .getMaxFromDataframeColumnOrZeroIfEmptyTable(myDF, "SKU") == 3,
            "Max SKU is 3")
    }


    test("Test readTableFromPathOrCreateEmptyDataframeFromSchema with data in folder") {
        val df = myTraitUnderTest
          .readTableFromPathOrCreateEmptyDataframeFromSchema(
              spark, "src/test/data/read_from_path/with_data", SKUMapperSchema)

        assert(df.schema.diff(SKUMapperSchema).isEmpty, "Schema should be SKUMapperSchema")
        assert(df.count() == 3, "Number of rows is 3")
    }


    test("Test readTableFromPathOrCreateEmptyDataframeFromSchema with NO data in folder") {
        val df = myTraitUnderTest
          .readTableFromPathOrCreateEmptyDataframeFromSchema(
              spark, "src/test/data/read_from_path/no_data", SKUMapperSchema)

        assert(df.schema.diff(SKUMapperSchema).isEmpty, "Schema should be SKUMapperSchema")
        assert(df.count() == 0, "Dataframe is empty")
    }


    test("Test readTableFromPathOrCreateEmptyDataframeFromSchema with NON-existing folder") {
        val df = myTraitUnderTest
          .readTableFromPathOrCreateEmptyDataframeFromSchema(
              spark, "src/test/data/read_from_path/no_folder", SKUMapperSchema)

        assert(df.schema.diff(SKUMapperSchema).isEmpty, "Schema should be SKUMapperSchema")
        assert(df.count() == 0, "Dataframe is empty")
    }
}
