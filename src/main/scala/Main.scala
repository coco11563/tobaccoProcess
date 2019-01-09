import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.{JedisImplSer, JedisUtils}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    //init done
    val host = conf.getString("redis.host")
    val port = conf.getString("redis.port")
//    val process = conf.getString("process.mad.value")
    val jedis = new JedisImplSer(JedisUtils.buildRedisConf(host, port))
    jedis.getJedis.hset("processed","1","2")
  }
}
