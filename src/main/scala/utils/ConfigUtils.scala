package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._

object ConfigUtils {
  implicit def mapConfigImplicit(string: String) : Map[String, String] = {
    string.split(",").map(_.split("->")).map(arr => (arr(0), arr(1))).toMap
  }

  implicit def arrMapConfigImplicit(arr: String) : List[Map[String, String]] = {
    arr.split(";")
      .map(mapConfigImplicit).toList
  }
  implicit def booleanConvert(string: String) : Boolean = {
    string.toLowerCase match {
      case "true" => true
      case "false" => false
      case _ => throw new Exception(s"wrong boolean type : $string")
    }
  }

  implicit def structTypeImplicit(string: String) : DataType = {
    string.toLowerCase match {
      case "string" => StringType
      case "long" => LongType
      case "integer" => IntegerType
      case "boolean" => BooleanType
      case _ => throw new Exception(s"wrong DataType type : $string")
    }
  }
  implicit def stringSchemaConfigImplicit(string: String) : StructField = {
    val t = string.split(",")
    StructField(t(0), structTypeImplicit(t(1)), booleanConvert(t(2)))
  }
  implicit def schemaConfigImplicit(string: String) : StructType = {
    StructType(string
      .split(";")
      .map(t => stringSchemaConfigImplicit(t))
    )
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val schema : StructType = conf.getString("test.schema")
    println(schema)
  }
}
