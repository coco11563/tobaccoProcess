package utils

import java.util

import org.apache.spark.sql.Row
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

object JedisUtils {
  def buildRedisClusterConf(redisHost : String, redisPort : String) : util.Set[HostAndPort] = {
    var setRet = new util.HashSet[HostAndPort]
    val hosts = redisHost.trim.split(",")
    val ports = redisHost.trim.split(",")
    for (h <- hosts) {
      for (p <- ports) {
        setRet.add(new HostAndPort(h, java.lang.Integer.valueOf(p)))
      }
    }
    setRet
  }
  def buildRedisConf(redisHost : String, redisPort : String) : HostAndPort = {
    new HostAndPort(redisHost, java.lang.Integer.valueOf(redisPort))
  }
  def resetRedis(jedisCluster: JedisCluster): Unit = {
    import scala.collection.JavaConversions._
    for (pool <- jedisCluster.getClusterNodes.values) {
      val jedis = pool.getResource
      try
        jedis.flushAll
      catch {
        case ex: Exception =>
          System.out.println(ex.getMessage)
      } finally if (jedis != null) jedis.close()
    }
  }

  def recordDuplicate(rows: Iterable[Row], primaryKey : String, jedisCluster: JedisCluster, tableName:String) :Row = {
    var f = rows.head //rows length >= 1
    if (rows.size < 2) return f //only one elem
    for (row <- rows) {
      jedisCluster.hset(tableName + "@" + row.getAs[String](primaryKey) ,tableName + "::", row.getAs[String](primaryKey)) //set son -> father(1 -> 1)
    }
    f
  }

  def getFather(key:String, keyFrom : String, jedis : Jedis): String = {
    val s = jedis.hget(keyFrom + "@" + key, "::")
    if (s == null || s == key) jedis.hget(keyFrom + "@" + key,"::")
    //not son of any return itself
    // I know you will feel confuse , just relax :-)
    else getFather(s, keyFrom,jedis)
  }

  def getOneLayerFather(key:String, keyFrom : String,jedis : Jedis): String = {
    jedis.hget(keyFrom + "@" + key,"::")
  }
}
