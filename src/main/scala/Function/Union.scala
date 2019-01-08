package Function

import Function.Process.conf
import org.apache.spark.rdd.RDD
import utils.MySQLUtils._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import utils.MySQLUtils

//two table merge by some rules
object Union {
  def dfMerge(thisTable : MySQLTable, otherTable : MySQLTable) : MySQLTable = {
    val schemaNew = thisTable.schema
    val schemaOld = otherTable.schema
    thisTable.sparkSession.createDataFrame(otherTable.rdd.map(MySQLUtils.buildRow(_, schemaOld, schemaNew)), schemaNew) union thisTable
  }

  def dfSelectMerge(df1 : MySQLTable, strNeed1 : Seq[String], df2 : MySQLTable, strNeed2 : Seq[String], fieldMap : Map[String, String]) : MySQLTable = {
    val dataFrame = dfSelect(df2, strNeed2)
    val dataFrameMain = dfSelect(df1, strNeed1)
    val dataFrame3 = dfSchemaMapping(dataFrame, fieldMap)
    dfMerge(dataFrameMain, dataFrame3)
  }

  def dfSelectMerge(mainDF : MySQLTable, df2 : MySQLTable,
                    strNeed2 : Seq[String], fieldMap : Map[String, String]) : MySQLTable = {
    val dataFrame = dfSelect(df2, strNeed2)
    val dataFrame3 = dfSchemaMapping(dataFrame, fieldMap)
    dfMerge(mainDF, dataFrame3)
  }
  def dfSelect(df : MySQLTable, field : Seq[String]) : MySQLTable = {
    df.select(field.map(new Column(_)) : _*)
  }

  def dfSchemaMapping(df : MySQLTable, map : Map[String, String]) : MySQLTable = {
    val schema = schemaMapping(df.schema, map)
    df.sqlContext.createDataFrame(df.rdd, schema)
  }


}
