package retailsales

import org.apache.spark.SparkConf
import java.io.File
import java.util.Properties
import scala.io.Source
import scala.reflect.io.Directory

object Functions {
    def getSparkAppConf(filePath: String): SparkConf = {
        val sparkAppConf = new SparkConf
        //Set all Spark Configs
        val props = new Properties
        props.load(Source.fromFile(filePath).bufferedReader())
        props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
        sparkAppConf
    }

    def folderCleaner(paths: List[String]): Unit = {
        def myDirectory(path: String): Directory = new Directory(new File(path))
        paths.foreach(myDirectory(_).deleteRecursively())
        paths.foreach(new File(_).mkdir())
    }
}
