import java.io.{File, FileReader}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import db.MySQLUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

class tobaccoMapping {

}
object tobaccoMapping {
  def main(args: Array[String]): Unit = {
    //init config
    val conf = ConfigFactory.load
//    conf.load(new FileReader(new File("resources/application.properties")))
    val matchFields = conf.getString("match.fields")
    val fields = conf.getString("field")
    val duplicateFields = conf.getString("fields.duplicate")
    val psnSplit = conf.getString("fields.split.psn.string")
    val orgSplit = conf.getString("fields.split.org.string")

    val serverUrl = conf.getString("database.url")
    val user = conf.getString("database.user")
    val passwd = conf.getString("database.password")
    val processTable = conf.getString("database.tableName")

    val driverName = conf.getString("database.driver.name")
    val driverType = conf.getString("database.driver.type")
    val driverPath = conf.getString("database.driver.path")

    val databaseORGTable = conf.getString("database.org.table.name")
    val databasePSNTable = conf.getString("database.psn.table.name")

    val initedNeeded = conf.getBoolean("database.init")

    val outputPSNTableName = conf.getString("database.output.person.name")
    val outputORGTableName = conf.getString("database.output.organization.name")

    val strategy = conf.getString("database.output.strategy")

    val beforeOrg = conf.getString("fields.duplicates.before.org")
    val beforePSN = conf.getString("fields.duplicates.before.name")

    val psnPSNName = conf.getString("fields.duplicates.psn.name")
    val psnORGName = conf.getString("fields.duplicates.psn.org")
    val orgName = conf.getString("fields.duplicates.org.name")


    val PSNFieldMapping = Map(beforeOrg -> psnORGName, beforePSN -> psnPSNName)
    val ORGFieldMapping = Map(beforeOrg -> orgName)
    //init done
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "32")
      .set("spark.executor.extraClassPath",driverPath)
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val table = MySQLUtils.openTable(sparkSession,
      serverUrl
      , user, passwd, processTable, driverName, driverType)
    val schema = table.schema
    val rows = table.rdd.flatMap(r =>
      MySQLUtils
      .rowFlatDouble(r, schema.fieldIndex(duplicateFields.split(",")(0)),
        schema.fieldIndex(duplicateFields.split(",")(1)),
        psnSplit,
        orgSplit))
    val df = sparkSession.createDataFrame(rows, schema)

    val uuid = utils.UUIDEvaluator.getInstance()

    val originRDD = df.select(fields.split(",").map(s => new Column(s)) : _*)
      .rdd
      .map(r => MySQLUtils.rowBuildID(r, uuid))

    val saveSchema = new StructType(Array(StructField("PERSON_ID", LongType, nullable = false)) ++
      fields.split(",").map(s => StructField(PSNFieldMapping.getOrElse(s, s), StringType, nullable = true))
    )

    println("data schema is list below")
    saveSchema.foreach(f => print(f.name + "|"))
    println()

    val originTable = sparkSession.createDataFrame(originRDD, saveSchema)
//    MySQLUtils.saveTable(originTable,outputPSNTableName, serverUrl
//      ,driverType,user,passwd,driverName,SaveMode.Overwrite)
//    originRDD.take(10).foreach(println(_))
    if (initedNeeded) {

    } else {

    }

    val initPSNDupTable = if(initedNeeded) MySQLUtils.openTable(sparkSession,
      serverUrl
      , user, passwd, databasePSNTable, driverName, driverType) else MySQLUtils.openTable(sparkSession,
      serverUrl
      , user, passwd, outputPSNTableName, driverName, driverType)

    val initORGDupTable = if(initedNeeded) MySQLUtils.openTable(sparkSession,
      serverUrl
      , user, passwd, databaseORGTable, driverName, driverType) else MySQLUtils.openTable(sparkSession,
      serverUrl
      , user, passwd, outputORGTableName, driverName, driverType)


    // -> dg_author <- insert(id)  => m_person_11
  }
}
