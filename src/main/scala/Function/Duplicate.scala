package Function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.{Jedis, JedisCluster}
import utils.{JedisImplSer, JedisUtils, MySQLUtils}

//Map from table to table (relation record)
object Duplicate {
  def duplicate(df : DataFrame, jedis: JedisImplSer, recordField : String, fields : String*) : DataFrame = {
    val schema = df.schema
    val rdd = df.rdd
    val temp = rdd.map(r => (fields.map(s => r.getAs[String](s)).map(MySQLUtils.buildHash(_)), (r.getAs[String](recordField),r)))
      .groupByKey
      .values
      .map(recordAndDuplicate(_, jedis))
    df.sparkSession.createDataFrame(temp, schema)
  }

  def Duplicate[T <: AnyVal](rdd : RDD[Row], keyFieldName : String, schema : StructType, fields : String*) : RDD[(Map[T, T], Row)] = ???
  def DuplicateMerge[T <: AnyVal] (rdd : Iterable[(T, Row)]) : (Map[T,T], Row) = ???
  def DuplicateBiasMerge[T <: AnyVal] (rdd : Iterable[(T, Row)], biasFunction : Row => Double) : (Map[T,T], Row) = ???

  def recordAndDuplicate(rows: Iterable[(String, Row)], jedis: JedisImplSer) :Row = {
    var f = rows.head //rows length >= 1
    if (rows.size < 2) return f._2 //only one elem
    for (row <- rows) {
      jedis.getJedis.set(row._1, f._1) //set son -> father(1 -> 1)
      f = (f._1, dupRowMerge(f._2, row._2))
    }
    f._2
  }

  def dupRowMerge(rows : Row*) : Row = {
    var f = rows.head //rows length >= 1
    if (rows.length < 2) return f //only one elem
    var father = f.toSeq.toArray
    for (index <- 1 until rows.length) {
      var row = rows(index)
      if (row != null && f != null){ //null pointer
        father = gatherDup(father, row.toSeq.toArray)
      }
    }
    Row.fromSeq(father.toSeq)
  }

  def gatherDup(a: Array[Any], b: Array[Any]): Array[Any] = {
    for (indi <- a.indices) {
      if (a(indi) == null)
        a.update(indi, b(indi))
    }
    a
  }

  def fkWash(id : String, jedisImplSer: JedisImplSer) : String = {
    JedisUtils.getFather(id, jedisImplSer)
  }
}
