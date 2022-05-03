package retailsales

import org.scalatest.FunSuite
import java.io._

class FunctionsTest extends FunSuite{

    test("test getSparkAppConf") {
        val sparkAppConf = Functions.getSparkAppConf("src/test/data/spark.conf")

        assert(sparkAppConf.get("spark.app.name") == "Retail Sales", "spark.app.name should be Retail Sales")
        assert(sparkAppConf.get("spark.master") == "local[3]", "spark.app.name should be Retail Sales")
    }

    test ("test folderCleaner") {
        // this is to ensure that the test folder exists and is empty
        val folderPath: String = "src/test/data/folderCleaner"
        val newInnerFolder: String = "innerFolder"
        new File(folderPath).mkdir()
        Functions.folderCleaner(List(folderPath))
        new File(folderPath + '/' + newInnerFolder).mkdir()

        def createFile(path: String): Unit = new PrintWriter(new File(path)).close()
        List("/hello.txt", "/" + newInnerFolder + "/hello.txt")
          .foreach(path => createFile(folderPath + path))

        val myFileListPreDeletion = new File(folderPath).listFiles()
        assert(myFileListPreDeletion.length == 2, "Number of elements at folderPath is 2")

        Functions.folderCleaner(List(folderPath))
        val myFileListPostDeletion = new File(folderPath).listFiles()
        assert(myFileListPostDeletion.isEmpty, "folder does NOT exist")
    }
}
