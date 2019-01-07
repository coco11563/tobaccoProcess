import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    //init done
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "32")
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  }


}
