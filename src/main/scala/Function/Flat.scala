package Function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

//flat one table to a table with more flat field
object Flat {
  def Flat(df : DataFrame, flatFunction : (Row, String) => List[Row], flatField : String*) : DataFrame = ???
  def Flat(rdd : RDD[Row], field : StructType, flatFunction : (Row, String) => List[Row], flatField : String*) : RDD[Row] = ???
}
