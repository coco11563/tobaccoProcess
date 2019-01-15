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
    * $.redis.recordName =>
    * the field we need to generate the splice record
    * @param processName
    * @param sparkSession
    * @param config
    * @param jedis
    */
  def processMergeAndDuplicateWithRedisRecord(processName : String, sparkSession: SparkSession, config: Config, jedis: JedisImplSer) : (String, DataFrame) = {
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
    val mappingTable : Boolean = config
      .getBoolean(s"$processName.mapping.isNeed")
    val recordRedisName : String = config
      .getString(s"$processName.redis.recordName")

    val mappingField : Map[String,String] =
      if (fieldMapName == "") Map[String,String]()
    else fieldMapName
    val finalSchema : StructType = if (tableSchema.contains(",")) tableSchema // if specific the table schema
    else if (tableSchema == "all")  // get all schema in the o_ table
      readTables
        .map(s => MySQLUtils
          .getTableSchema(sparkSession, s))
        .reduce((a,b) => {
          var ret = new StructType()
          var set : Set[String] = Set[String]()
          for (f1 <- b) {
            val name = mappingField.getOrElse(f1.name, f1.name)
            if (!set.contains(name)) {
              set += name
              ret = ret.add(f1)
            }
          }
          for (f1 <- a) {
            val name = mappingField.getOrElse(f1.name, f1.name)
            if (!set.contains(name)) {
              set += name
              ret = ret.add(f1)
            }
          }
          val mapType = MySQLUtils.schemaMapping(ret, mappingField)
          if (mapType.fieldNames.contains("source_table")) {
            MySQLUtils.schemaMapping(mapType, Map("source_table" -> "SOURCE_TABLE"))
          } else if (!mapType.fieldNames.contains("source_table".toUpperCase)) {
            mapType
          } else {
            mapType.add("SOURCE_TABLE", StringType)
          }
        })
      else // get table schema from m_ table complement
      //
        MySQLUtils.getTableSchema(sparkSession, tableSchema)


    println(finalSchema)

    var dfOrigin : DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],
      finalSchema)

    for (i <- readTables.indices) {
      val tableName = readTables(i)
      val df = MySQLUtils.openTable(sparkSession, tableName)

      val beforeSchema = df.schema
      val schemaWithSource = beforeSchema.add("SOURCE_TABLE", StringType)
      println(schemaWithSource)
      val dfWithSource = sparkSession.createDataFrame(
        df.rdd.map(MySQLUtils.buildRow(_, beforeSchema, schemaWithSource))
          .map(MySQLUtils.rowChangeValue(_, schemaWithSource.indexOf("SOURCE_TABLE"), tableName))
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
      jedis.getJedis.hset("processed", processName, tableName)
    }
    val af = Duplicate.duplicate(dfOrigin, jedis, idFieldName, duplicateOn : _*)
    af.show(10)
    println("init the record redis cache")
    println(s"starting at ${System.currentTimeMillis()}")
    println(s"field is $recordRedisName")
    println(s"schema is ${af.schema}")
    JedisUtils.keyFieldPersist(af, jedis, processName, idFieldName, recordRedisName)
    //(saving temp table)
    MySQLUtils.saveCsv(MySQLUtils.washForCsv(af),s"/out/csv/tmp/$processName/")
    HDFSUtils.mergeFileByShell(s"/out/csv/tmp/$processName/*",s"/data/out/csv/tmp/${processName}_entity.csv")
    (processName, af)
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
    // read table -> get two field -> mk df -> record the mapping -> save As CSV
  def relationshipProcess(processName : String, sparkSession: SparkSession
                          , config: Config, jedis: JedisImplSer) : (String, DataFrame) ={
    if (!config.getBoolean(s"$processName.relationship.needProcess")) return (null, null)

    val tableName :List[String] = config.getString(s"$processName.relationship.tableNames").split(",").toList
    val fieldPair : List[(String,String)] = config.getString(s"$processName.relationship.fieldPairs").split(";").map(s => {
      val ret = s.split(",")
      (ret(0), ret(1))
    }).toList
//    val relationshipName : List[String] = config.getString(s"$processName.relationship.names").split(",").toList
    val schema : StructType = new StructType()
      .add("from",StringType, nullable = false)
      .add("to",StringType, nullable = false)
    var emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    for (i <- tableName.indices) {
      val df = MySQLUtils.openTable(sparkSession, tableName(i))
      val bak = Union.dfSelect(df, Seq(fieldPair(i)._1, fieldPair(i)._2))
      val rdd = bak.rdd.map(r => {Row.fromSeq(r.toSeq.map(a => Duplicate.fkWash(a.asInstanceOf[String], jedis)))})
      val table = sparkSession.createDataFrame(rdd, schema)
      emptyDF = Union.dfMerge(emptyDF, table)
    }
    println("init the record redis cache")
    println(s"starting at ${System.currentTimeMillis()}")
    JedisUtils.relationPersist(emptyDF, jedis, processName)
    println(s"ending at ${System.currentTimeMillis()}")
    MySQLUtils.saveCsv(emptyDF,s"/out/csv/$processName/rel")
    HDFSUtils.mergeFileByShell(s"/out/csv/$processName/rel/*",s"/data/out/csv/${processName}_relationship.csv")
    (processName, emptyDF)
  }
    // read table -> dup -> record the id | field -> saveAsCsv
  def simpleTableProcessWithRedisRecord(processName : String, sparkSession: SparkSession,
                                        config: Config, jedisImplSer: JedisImplSer) :Unit = {
    val tableName : String = config.getString(s"$processName.item.tableName")
    val needField : Seq[String] = config.getString(s"$processName.item.tableFields").split(",").toSeq
    val dupField : Array[String] = config.getString(s"$processName.item.duplicateField").split(",")
    val recordRedisName : String = config
      .getString(s"$processName.redis.recordName")
    val id : String =  config.getString(s"$processName.item.idField")
    val df = MySQLUtils.openTable(sparkSession, tableName)
    val bak = Union.dfSelect(df, needField)
    val wrt = Duplicate.duplicate(bak, jedisImplSer, id, dupField : _*)
    println("init the record redis cache")
    println(s"starting at ${System.currentTimeMillis()}")
    JedisUtils.keyFieldPersist(wrt, jedisImplSer, processName, id, recordRedisName)
    MySQLUtils.saveCsv(wrt,s"/out/csv/$processName/")
    HDFSUtils.mergeFileByShell(s"/out/csv/$processName/*",s"/data/out/csv/${processName}_entity.csv")
  }
  // reset the HDFS and redis
  def resetProcess(jedisImplSer: JedisImplSer) : Unit = {
    HDFSUtils.deleteFile(HDFSUtils.HDFSFileSystem, "/out/csv")
    HDFSUtils.deleteFile(HDFSUtils.HDFSFileSystem, "/data/out")
    JedisUtils.resetRedis(jedisImplSer.getJedis)
  }
  //(read temp table -> get weaving field -> adding field
  // -> read m_table_complement -> hashing two table
  // -> get differ field -> build final field
  // -> final MTable)
  def finalMTableBuild(processName : String,
                       path : String, sparkSession: SparkSession, config: Config) : Unit = {
    val csvDF =sparkSession
      .sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter","\t")
      .option("quote","\"")
      .load(path)
    val weavingFile = config.getString("")
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
    val simpleProcess = conf.getString("process.simple.value")
    val jedis = new JedisImplSer(JedisUtils.buildRedisConf(host, port))
    resetProcess(jedis)
    if (process != "")
    process.split(",").foreach(str => {
      processMergeAndDuplicateWithRedisRecord(str, sparkSession, conf, jedis)
//      relationshipProcess(str, sparkSession, conf, jedis)
    })
    if (simpleProcess != "")
    simpleProcess.split(",").foreach(str => {
      simpleTableProcessWithRedisRecord(str, sparkSession, conf, jedis)
//      relationshipProcess(str, sparkSession, conf, jedis)
    })
  }
}
