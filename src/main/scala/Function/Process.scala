package Function


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import utils.ConfigUtils._
import utils.{JedisImplSer, JedisUtils, MySQLUtils}
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

  def processMergeAndDuplicate(processName : String, sparkSession: SparkSession, config: Config, jedis: JedisImplSer) : Unit = {
    val readTables : List[String] = config.getString(s"$processName.item.tableNames").split(",").toList
    val needFields : List[String] = config.getString(s"$processName.item.tableFields").split(";").toList
    val mappingFieldName : List[Map[String, String]] = config.getString(s"$processName.item.fieldMap")
    val idFieldName : String = config.getString(s"$processName.item.idField")
    val duplicateOn : List[String] = config.getString(s"$processName.item.duplicateField").split(",").toList
    val finalSchema : StructType = config.getString(s"$processName.item.schema")
    val saveName : String = config.getString(s"$processName.item.outName")
    val mappingTable : Boolean = config.getString(s"$processName.mapping.isNeed")
    var dfOrigin : DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], finalSchema)
    for (i <- readTables.indices) {
      val tableName = readTables(i)
      val df = MySQLUtils.openTable(sparkSession, tableName)
      val mappingField = mappingFieldName(i)
      val needField = needFields(i)
      dfOrigin = Union.dfSelectMerge(dfOrigin, df, needField.split(","), sparkSession, mappingField)
    }
    if (mappingTable) {
      val mapName : String = config.getString(s"$processName.mapping.mapName")
      val schema = dfOrigin.schema
      dfOrigin = sparkSession.createDataFrame(mapping(processName, sparkSession, dfOrigin.rdd, schema.indexOf(mapName)), schema)
    }
    val af = Duplicate.duplicate(dfOrigin, jedis.getJedis, idFieldName, duplicateOn : _*)
    MySQLUtils.saveTable(af, saveName, SaveMode.Overwrite)
  }

  def mapping(processName : String, sparkSession: SparkSession, rows:RDD[Row], mapIndex : Int): RDD[Row] = {
    val mappingTableName = conf.getString(s"$processName.mapping.table")
    val mappingBeforeField = conf.getString(s"$processName.mapping.fields")
    val mappingAfterField = conf.getString(s"$processName.mapping.tofields")
    val mappingTable = MySQLUtils.openTable(sparkSession, mappingTableName)
      .select(new Column(mappingBeforeField), new Column(mappingAfterField))
      .rdd
      .map(row => (row.getAs[String](mappingBeforeField), row.getAs[String](mappingAfterField)))
      .collect()
      .toMap
    val mapping = sparkSession.sparkContext.broadcast(mappingTable)
    rows.map(r => MySQLUtils.rowMappingValue(r, mapIndex, mapping.value))
  }
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    //init done
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "32")
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val host = conf.getString(s"${args(0)}.redis.host")
    val port = conf.getString(s"${args(0)}.redis.port")

    val jedis = new JedisImplSer(JedisUtils.buildRedisConf(host, port))
    processMergeAndDuplicate(args(0), sparkSession, conf,jedis)
  }
}
