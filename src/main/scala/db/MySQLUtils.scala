package db
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import utils.UUIDEvaluator

import scala.collection.mutable.ListBuffer

object MySQLUtils {
    type MySQLTable = DataFrame

  /**
    *
    * @param ss sparkSession
    * @param serverUrl mysql URL
    * @param user mysql Username
    * @param password mysql Password
    * @param tbName table name(db should be specific in url)
    * @return
    */
  def openTable (ss : SparkSession, serverUrl : String, user:String , password : String, tbName : String, driverName : String, driverType : String) : MySQLTable = {
    ss.sqlContext.read
      .format(driverType)
      .option("url", serverUrl)
      .option("dbtable",tbName)
      .option("user", user)
      .option("driver", driverName)
      .option("password", password)
      .load
  }
  def rowChangeValue(row : Row, index : Int, value : String) : Row ={
    val li = ListBuffer[Any]()
    var i = 0
    for (v <- row.toSeq.toList) {
      if (i == index) li += value
      else li += v
      i += 1
    }
    Row.fromSeq(li)
  }
  /**
    * * val firstValue = row(0)
    * * // firstValue: Any = 1
    * * val fourthValue = row(3)
    * * // fourthValue: Any = null
    * @param row specific row
    * @param rowIndex index of the column, which need split
    * @param splitString split value
    * @return the flat rows
    */
  def rowFlatMap(row : Row, rowIndex : Int, splitString : String) : List[Row] = {
    val rowValue = row.getAs[String](rowIndex)
    if (rowValue == null) List(row)
    else {
      for (str <- rowValue.split(splitString)) yield {
        rowChangeValue(row, rowIndex, str)
      }
    }.toList
  }

  def rowSetNull(row: Row, rowIndex : Int) : Row = {
    rowChangeValue(row, rowIndex, null)
  }

  //依据设计好的规则进行flat
  def rowFlatDouble(row : Row, rowIndex : Int, secRowIndex : Int,
                    splitString : String, secSplitString : String) : List[Row] = {
    val value = row.getAs[String](rowIndex)
    val valueSep = row.getAs[String](secRowIndex)
    if (value == null || valueSep == null) rowFlatMap(row, rowIndex, splitString)
    else {
      val valueArray = value.split(splitString)
      val valueSepArray = valueSep.split(secSplitString)
      //case one : same num 1:1
      if (valueArray.length == valueSepArray.length) {
        rowFlatDoubleArray(row, valueArray, valueSepArray, rowIndex, secRowIndex)
      }
      //case two : n : 1
      else if (valueSepArray.length == 1) {rowFlatMap(row, rowIndex, splitString)}
      //case three : n : m
      else {
        rowFlatMap(row, rowIndex, splitString).map(r => rowSetNull(row, secRowIndex))
      }
    }
  }

  def rowFlatDoubleArray(row : Row, strArr : Array[String],
                         secStrArr : Array[String], rowIndex : Int,
                         secRowIndex : Int) : List[Row] = {
    val ret = for ((a:String,b:String) <- strArr zip secStrArr) yield {
      rowChangeValue(rowChangeValue(row, rowIndex, a), secRowIndex, b)
    }
    ret.toList
  }
  //set an uuid long in head of row
  //side effect will occur
  def rowBuildID(row : Row, uuid: UUIDEvaluator) : Row = {
    Row.fromSeq(Seq(uuid.getId(1)) ++ row.toSeq)
  }

  def rowMerge(mainRows : MySQLTable, insertRows: MySQLTable, fieldNames : String*) : MySQLTable = ???

  def saveTable(df : MySQLTable, table: String,
                serverUrl : String, driverType : String,
                prop : Properties, mode : SaveMode): Unit = {
    df.write
      .mode(mode)
      .format(driverType)
      .jdbc(serverUrl, table, prop)
  }

  def saveTable(df : MySQLTable, table: String,
                serverUrl : String, driverType : String,
                user:String , password : String, driverName : String , mode : SaveMode): Unit = {
    val prop = new Properties()
    prop.put("url", serverUrl)
    prop.put("dbtable",table)
    prop.put("user", user)
    prop.put("driver", driverName)
    prop.put("password", password)
    saveTable(df, table, serverUrl, driverType, prop, mode)
  }


}
