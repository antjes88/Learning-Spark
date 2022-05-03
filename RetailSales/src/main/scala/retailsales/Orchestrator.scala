package retailsales

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import Functions._


object Orchestrator extends Serializable {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            logger.error("Usage: RetailSales filename")
            System.exit(1)
        }

        logger.info("Parameters definition")
        val productDimensionPath: String = "data_lake/edw/retail_sales/product"
        val retailerDimensionPath: String = "data_lake/edw/retail_sales/retailer"
        val skuEDWPath: String = if (args.length == 2) args(1) else "data_lake/edw/etl/sku_mapper"

        logger.info("Starting spark session")
        val spark = SparkSession.builder()
          .config(getSparkAppConf("spark.conf"))
          .getOrCreate()

        logger.info("Initializing Pipeline class")
        val pipelineRunner = new MatPipeline(
            spark, skuEDWPath, args(0), productDimensionPath, retailerDimensionPath)

        logger.info("Loading Product Dimension")
        pipelineRunner.loadProductDimension()

        logger.info("Loading Retailer Dimension")
        pipelineRunner.loadRetailerDimension()

        if (spark.sparkContext.appName == getSparkAppConf("spark.conf").get("spark.app.name")) {
            spark.stop()
            logger.info("Spark session finished")
        }
    }
}
