package Function


import Function.Union.dfSchemaMapping
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructType}
import utils.ConfigUtils._
import utils.{HDFSUtils, JedisImplSer, JedisUtils, MySQLUtils}
object Process {
  val conf: Config = ConfigFactory.load

  def projectProcess(conf : Config) : Unit = {
    val readTable : List[String] = conf.getString("project.item.tableNames").split(",").toList
    val needField : List[String] = conf.getString("project.item.tableFields").split(";").toList
    val mappingFieldName : List[Map[String, String]] = conf.getString(s"project.item.fieldMap")
    val pair = readTable zip needField
    for ((table : String, fields : String) <- pair) {
      println(table + ":" + fields)
    }
    println(mappingFieldName)
  }

  /**
    * config description
    * $.item.tableNames =>
    * all the table name, which need process eg. o_xxx,o_yyy,o_zzz
    * $.item.tableFields =>
    * the fields which list in tableNames eg. a,b,c;d,e,f
    * $.item.fieldMap =>
    * the field mapping config, by the implicit,
    * we can easily translate the string into mapping.
    * eg.a->b,c->d,e->f
    * $.item.idField =>
    * the id field name **after field mapping!**,
    * this field will be used to record the duplicate deletion eg. a
    * $.item.duplicateField =>
    * the field which need duplicate by eg. a,b,c
    * $.item.schema =>
    * the final schema of middle table eg. a,string,true;b,long,false
    * $.item.outName =>
    * name of the output table
    * $.mapping.isNeed =>
    * are there any field need mapping? eg. true/false
    * $.mapping.mapName =>
    * the field which need mapping
    * $.mapping.table =>
    * mapping table -> (field -> toField)
    * $.mapping.field =>
    * the fields need mapping
    * $.mapping.toField =>
    * the after mapping field
    *
    * @param processName
    * @param sparkSession
    * @param config
    * @param jedis
    */
  def processMergeAndDuplicate(processName : String, sparkSession: SparkSession, config: Config, jedis: JedisImplSer) : Unit = {
    val readTables : List[String] = config
      .getString(s"$processName.item.tableNames").split(",").toList
    val needFields : List[String] = config
      .getString(s"$processName.item.tableFields").split(";").toList
    val fieldMapName : String = config
      .getString(s"$processName.item.fieldMap")
    val idFieldName : String = config
      .getString(s"$processName.item.idField")
    val duplicateOn : List[String] = config
      .getString(s"$processName.item.duplicateField").split(",").toList
    val tableSchema : String = config
      .getString(s"$processName.item.schema")
    val saveName : String = config
      .getString(s"$processName.item.outName")
    val mappingTable : Boolean = config
      .getBoolean(s"$processName.mapping.isNeed")

    val finalSchema : StructType = if (tableSchema.contains(",")) tableSchema else MySQLUtils.getTableSchema(sparkSession, tableSchema)

    var dfOrigin : DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], finalSchema)

    val mappingField : Map[String,String] =
      if (fieldMapName == "") Map[String,String]()
    else fieldMapName

    for (i <- readTables.indices) {
      val tableName = readTables(i)
      val df = MySQLUtils.openTable(sparkSession, tableName)

      val beforeSchema = df.schema
      val schemaWithSource = beforeSchema.add("source_table", StringType)
      println(schemaWithSource)
      val dfWithSource = sparkSession.createDataFrame(
        df.rdd.map(MySQLUtils.buildRow(_, beforeSchema, schemaWithSource))
          .map(MySQLUtils.rowChangeValue(_, schemaWithSource.indexOf("source_table"), tableName))
      , schemaWithSource)
      val needField = needFields(i)

      var dfO = if (!(needField == "all"))
        dfSchemaMapping(Union.dfSelect(dfWithSource, needField.split(",")), mappingField)
      else dfSchemaMapping(dfWithSource, mappingField)


      if (mappingTable) {
        val mapName : String = config.getString(s"$processName.mapping.mapName")
        val schema = dfO.schema
        dfO = sparkSession.createDataFrame(mapping(processName, sparkSession, dfO.rdd, schema.indexOf(mapName)), schema)
      }

      dfOrigin = Union.dfMerge(dfOrigin, dfO)
    }
    val af = Duplicate.duplicate(dfOrigin, jedis, idFieldName, duplicateOn : _*)
    af.show(10)
    MySQLUtils.saveCsv(af,s"/out/csv/$processName/")
    HDFSUtils.mergeFileByShell(s"/out/csv/$processName/*",s"/data/out/csv/${processName}_entity.csv")
  }


  def mapping(processName : String, sparkSession: SparkSession, rows:RDD[Row], mapIndex : Int): RDD[Row] = {
    val mappingTableName = conf.getString(s"$processName.mapping.table")
    val mappingBeforeField = conf.getString(s"$processName.mapping.field")
    val mappingAfterField = conf.getString(s"$processName.mapping.toField")
    val mappingTable = MySQLUtils.openTable(sparkSession, mappingTableName)
      .select(new Column(mappingBeforeField), new Column(mappingAfterField))
      .rdd
      .map(row => (row.getAs[String](mappingBeforeField), row.getAs[String](mappingAfterField)))
      .collect()
      .toMap
    val mapping = sparkSession.sparkContext.broadcast(mappingTable)
    rows.map(r => MySQLUtils.rowMappingValue(r, mapIndex, mapping.value))
  }

  def relationshipProcess(sparkSession: SparkSession, config: Config, jedis: JedisImplSer) : Unit ={
    val tableName :List[String] = config.getString(s"relationship.tableNames").split(",").toList
    val fieldPair : List[(String,String)] = config.getString(s"relationship.fieldPairs").split(";").map(s => {
      val ret = s.split(",")
      (ret(0), ret(1))
    }).toList
    val relationshipName : List[String] = config.getString(s"relationship.tableName").split(",").toList
    for (i <- tableName.indices) {
      val df = MySQLUtils.openTable(sparkSession, tableName(i))
      val bak = Union.dfSelect(df, Seq(fieldPair(i)._1, fieldPair(i)._2))
      val schema = bak.schema
      val rdd = bak.rdd.map(r => {Row.fromSeq(r.toSeq.map(a => Duplicate.fkWash(a.asInstanceOf[String], jedis)))})
      val table = sparkSession.createDataFrame(rdd, schema)
      MySQLUtils.saveCsv(table,s"/out/csv/${relationshipName(i)}/")
      HDFSUtils.mergeFileByShell(s"/out/csv/${relationshipName(i)}/*",s"/data/out/csv/m_${relationshipName(i)}.csv")
    }
  }

  def simpleTableProcess(processName : String, sparkSession: SparkSession, config: Config, jedisImplSer: JedisImplSer) :Unit = {
    val tableName : String = config.getString(s"$processName.item.tableName")
    val needField : Seq[String] = config.getString(s"$processName.item.tableFields").split(",").toSeq
    val dupField : Array[String] = config.getString(s"$processName.item.duplicateField").split(",")
    val id : String =  config.getString(s"$processName.item.idField")
    val df = MySQLUtils.openTable(sparkSession, tableName)
    val bak = Union.dfSelect(df, needField)
    val wrt = Duplicate.duplicate(bak, jedisImplSer, id, dupField : _*)
    MySQLUtils.saveCsv(wrt,s"/out/csv/$processName/")
    HDFSUtils.mergeFileByShell(s"/out/csv/$processName/*",s"/data/out/csv/${processName}_entity.csv")
  }
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    //init done
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "32")
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val host = conf.getString("redis.host")
    val port = conf.getString("redis.port")
    val process = conf.getString("process.mad.value")
    val jedis = new JedisImplSer(JedisUtils.buildRedisConf(host, port))
    JedisUtils.resetRedis(jedis.getJedis)
    process.split(",").foreach(str => processMergeAndDuplicate(str, sparkSession, conf, jedis))
  }
}
