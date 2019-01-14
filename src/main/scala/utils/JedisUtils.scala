package utils

import java.util

import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}
import scala.collection.JavaConversions._
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

  def resetRedis(jedis: Jedis): Unit = {
    try
      jedis.flushAll
    catch {
      case ex: Exception =>
        System.out.println(ex.getMessage)
    } finally if (jedis != null) jedis.close()
  }

  def recordDuplicate(rows: Iterable[Row], primaryKey : String, jedisCluster: JedisCluster) :Row = {
    var f = rows.head //rows length >= 1
    if (rows.size < 2) return f //only one elem
    for (row <- rows) {
      jedisCluster.set(row.getAs[String](primaryKey) , row.getAs[String](primaryKey)) //set son -> father(1 -> 1)
    }
    f
  }

  // new
  def keyFieldPersist(df : DataFrame, jedisImplSer: JedisImplSer, fieldName : String
                      , idName :String, fields : String *) : Unit = {
    val schema = df.schema
    val index = fields.map(s => schema.indexOf(s))
    val idIndex = schema.indexOf(idName)
    val rdd = df.rdd
    rdd
      .map(r => (r.getAs[String](idName),
        index
          .map(i => r.getAs[String](i))
          .reduce(_ + "|" + _)))
      .foreach(str => {
        jedisImplSer.getJedis.hset(str._1,fieldName ,str._2)
      })
  }
  // new
  def relationPersist(df : DataFrame, jedisImplSer: JedisImplSer, fieldName : String) : Unit = {
    df.rdd
      .foreach(r => {
        val key = r.getAs[String](0)
        val value = r.getAs[String](1)
        val map = jedisImplSer.getJedis.hgetAll(key)
        if (map == null) jedisImplSer.getJedis.hmset(key, Map[String,String](fieldName, value))
        else {
          val old = map.get(fieldName)
          if (old == null) {
            map.put(fieldName, value)
          } else {
            map.put(fieldName, value + "," + old)
          }
          jedisImplSer.getJedis.hmset(key, map)
        }
      })
  }
  // new
  def getKeyField(jedisImplSer: JedisImplSer, key : String, field : String) : String = {
    val keys = jedisImplSer.getJedis.hmget(key, field)(0)
    if (keys == null) null
    else {
      val list = keys.split(",")
      list.map(str => jedisImplSer.getJedis.hget(str, field)).reduce(_ + "," + _)
    }
  }

  def getFather(key:String, jedis : JedisImplSer): String = {
    val s = jedis.getJedis.get(key)
    if (s == null || s == key) jedis.getJedis.get(key)
    //not son of any return itself
    // I know you will feel confuse , just relax :-)
    else getFather(s, jedis)
  }

  def getOneLayerFather(key:String ,jedis : Jedis): String = {
    jedis.get(key)
  }
}
