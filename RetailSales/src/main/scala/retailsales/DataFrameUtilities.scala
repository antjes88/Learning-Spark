package retailsales

import org.apache.spark.sql.functions.{col, isnull, max}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import java.io.File

trait DataFrameUtilities {
    def checkDuplicationOnColumn(df: DataFrame, column: String, tableName: String): Unit = {
        if (df.count() != df.select(column).distinct().count()) {
//            logger.error(s"ETL: $tableName - $column must NOT have duplicates")
            throw new Exception(s"ETL: $tableName - $column must NOT have duplicates")
        }
    }

    def checkColumnWithoutNulls(df: DataFrame, column: String, tableName: String): Unit = {
        if (df.where(isnull(col(column))).count() > 0) {
//            logger.error(s"ETL: $tableName - $column must NOT have null values")
            throw new Exception(s"ETL: $tableName - $column must NOT have null values")
        }
    }

    def getMaxFromDataframeColumnOrZeroIfEmptyTable(df: DataFrame, column: String): Any = {
        if (df.count() == 0) {
            0
        } else {
            df.select(max(column))
              .rdd.map(row => row(0)).collect().toList.head
        }
    }

    def readTableFromPathOrCreateEmptyDataframeFromSchema(spark: SparkSession, tablePath: String,
                                                          schema: StructType): DataFrame = {
         val myFile = new File(tablePath)
         if (myFile.isDirectory)
             if (myFile.listFiles().length > 0)
                 if (myFile.listFiles().exists(_.toString.endsWith(".parquet")))
                     return spark.read
                       .format("parquet")
                       .load(tablePath)

        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
}
