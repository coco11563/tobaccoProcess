package utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object MySQLUtils {
  val conf: Config = ConfigFactory.load()
  val serverUrl: String = conf.getString("database.url")
  val user: String = conf.getString("database.user")
  val passwd: String = conf.getString("database.password")
  val driverName: String = conf.getString("database.driver.name")
  val driverType: String = conf.getString("database.driver.type")

  type MySQLTable = DataFrame
  type DuplicatedFunction = (MySQLTable, String *) => MySQLTable
  type RecordFunction = (String, String) => Unit

  val uuid: UUIDEvaluator = UUIDEvaluator.getInstance()
  /**
    *
    * @param ss sparkSession
    * @param tbName table name(db should be specific in url)
    * @return
    */
  def openTable (ss : SparkSession, tbName : String) : MySQLTable = {
    ss.sqlContext.read
      .format(driverType)
      .option("url", serverUrl)
      .option("dbtable",tbName)
      .option("user", user)
      .option("driver", driverName)
      .option("password", passwd)
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
  def rowMappingValue(row : Row, index : Int, map :  Map[String,String]) : Row ={
    val li = ListBuffer[Any]()
    var i = 0
    for (v <- row.toSeq.toList) {
      if (i == index) li += map.getOrElse(v.asInstanceOf[String] ,v)
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
    Row.fromSeq(Seq(String.valueOf(uuid.getId(1))) ++ row.toSeq)
  }

  def schemaMapping(schema : StructType, map: Map[String, String]) : StructType = {
    new StructType(
      schema.map(s => StructField(map.getOrElse(s.name, s.name.toUpperCase), s.dataType, s.nullable)).toArray
    )
  }

  def rowMerge(ss : SparkSession, mainRows : MySQLTable, insertRows: MySQLTable,productIdName : String, psnIdName : String, orgName : String, fieldNames : String*) : (MySQLTable, MySQLTable, MySQLTable) = {
    val newSchema = mainRows.schema
    println()
    newSchema.foreach(s => println(s.name))
    println("---------")
    val oldSchema = insertRows.schema
    oldSchema.foreach(s => println(s.name))
    val mainFieldIndex = fieldNames.map(s => newSchema.fieldIndex(s))
    val insertFieldIndex = fieldNames.map(s => oldSchema.fieldIndex(s))
    val productIndex = oldSchema.fieldIndex(productIdName)
    val idIndex = newSchema.fieldIndex(psnIdName)
    val orgIndex = oldSchema.fieldIndex(orgName)
    val mainRdd : RDD[Row] = mainRows.rdd.map(r => {
      if(r.getAs[String](psnIdName) == "") rowChangeValue(r, idIndex, String.valueOf(uuid.getId(0)))
      else r
    })
    val show = insertRows.rdd.map(row =>
      (buildHash(insertFieldIndex.map(row.getAs[String]) : _*) ,(
      buildRow(row, oldSchema, newSchema), false,
      row.getAs[String](productIndex), row.getAs[String](orgIndex)
    ))).take(10)
    show.foreach(a => println(a._2._1))
    val tmp = insertRows
      .rdd
      .map(row =>
        (buildHash(insertFieldIndex.map(row.getAs[String]) : _*) ,
        ( buildRow(row, oldSchema, newSchema), false,
          row.getAs[String](productIndex),
          row.getAs[String](orgIndex)
        )))
      .union(mainRdd
        .map(row => (buildHash(mainFieldIndex.map(row.getAs[String]) : _*) ,(row, true, row.getAs[String](productIndex),row.getAs[String](orgIndex))))
      ).groupByKey
      .values
      .map(a => combineRow(a))

    val proRel = tmp.map(a => (a._1.getAs[String](idIndex), a._2))
        .map(a => {
          a._2.map((a._1,_)) //psn -> product
        }).flatMap(_.toList).map(s => Row.fromSeq(Seq(s._1, s._2)))
    val orgRel = tmp.map(a => (a._1.getAs[String](idIndex), a._3))
      .map(a => {
        a._2.map((a._1,_)) //psn -> product
      }).flatMap(_.toList).map(s => Row.fromSeq(Seq(s._1, s._2)))
    (
      ss.createDataFrame(tmp.map(_._1), newSchema),  //spec row
      ss.createDataFrame(proRel, new StructType(Array(psnIdName, productIdName).map(s => StructField(s, StringType, nullable = true)))), //rel row
      ss.createDataFrame(orgRel, new StructType(Array(psnIdName, orgName).map(s => StructField(s, StringType, nullable = true))))
    )
  }

  def combineRow(iterable: Iterable[(Row, Boolean, String, String)]) : (Row, Set[String], Set[String]) = {
    val li = iterable.toList
    var set = Set[String]()
    var set_2 = Set[String]()
    val row = li.reduce((a, b) => {
      if (a._2) {
        set += b._3
        set_2 += b._4
        a
      } else if (b._2) {
        set += a._3
        set_2 += b._4
        b
      } else {
        set += b._3
        set_2 += b._4
        a
      }
    })
    (row._1, set,set_2)
  }
  def getTableSchema(sparkSession: SparkSession, tableName : String) : StructType = {
    openTable(sparkSession, tableName).schema
  }
  def buildRow(row: Row, oldSchema : StructType, newSchema: StructType) : Row = {
    val seq : Seq[String]= for (str <- newSchema) yield {
      if (oldSchema.contains(str)) {
        row.getAs[String](str.name)
      } else null
    }
    Row.fromSeq(seq)
  }

  def buildHash(fields : String *): Int = {
    fields.foldLeft("")(_ + _).##
  }

  def saveTable(df : MySQLTable, table: String,
                serverUrl : String, driverType : String,
                prop : Properties, mode : SaveMode): Unit = {
    df.write
      .mode(mode)
      .format(driverType)
      .jdbc(serverUrl, table, prop)
  }
  def saveCsv(df : MySQLTable, outName : String ) : Unit = {
    df.write
      .option("header","true")
      .option("delimiter","\t")
      .option("quoteMode", "NON_NUMERIC")
      .option("quote","\"")
      .csv(outName)
  }
  def saveTable(df : MySQLTable, table: String,
               mode : SaveMode): Unit = {
    val prop = new Properties()
    prop.put("url", serverUrl)
    prop.put("dbtable",table)
    prop.put("user", user)
    prop.put("driver", driverName)
    prop.put("password", passwd)
    saveTable(df, table, serverUrl, driverType, prop, mode)
  }

}
