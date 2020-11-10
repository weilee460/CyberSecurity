//*******************************
//Spark Hive demo
//******************************

import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
case class Record(key: Int, value: String)

object SparkHiveDemo {
  def main(args: Array[String]) {
    println("hello Scala in Spark")
    sparkReadHive()
  }

  /**
   *  ~/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --class "SparkHiveDemo"  --master yarn --deploy-mode client
   *  --driver-memory 2g --executor-memory 4g --executor-cores 1 sparkhivedemo_2.12-0.1.jar
   *
   */

  //Test Spark read data from Hive
  def sparkReadHive(): Unit = {
    val spark_conf = new SparkConf()
    spark_conf.setMaster("yarn")

    val spark = SparkSession.builder.appName("Spark Access Hive Test")
      .config(spark_conf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("Spark Version:" + spark.version)

    import spark.implicits._
    import spark.sql

    //sql("SELECT COUNT(*) FROM testdb.test_table1").show()

    val someDataDF = sql("SELECT * FROM testdb.test_table1 limit 10")
    someDataDF.show(false)

    // Save DataFrame to the Hive managed table
    // someDataDF.write.mode("overwrite").saveAsTable("test_table11")

    // Save DataFrame to the Hive extern table
    someDataDF.drop("y").drop("m").drop("d")
      .write.option("path", "/databases/testdb/test_table22")
      .mode("overwrite")
      .saveAsTable("testdb.test_table22")

    // spark.sql("DROP TABLE IF EXISTS test_table22")

    spark.stop()
  }
}