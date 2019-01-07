import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import utils.MySQLUtils

class tobaccoMapping {

}
object tobaccoMapping {
  def main(args: Array[String]): Unit = {
    //init config
    val conf = ConfigFactory.load
    //init done
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "32")
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    for (pro <- args) {
      println(s"Now we processing the $pro")
      mainProcess(sparkSession, conf, pro)
    }
    // -> dg_author <- insert(id)  => m_person_11
  }


  def mainProcess (sparkSession: SparkSession, conf : Config, process : String) : Unit = {
    val matchFields = conf.getString(s"$process.match.fields")
    val fields = conf.getString(s"$process.field")
    val duplicateFields = conf.getString(s"$process.fields.duplicate")
    val psnSplit = conf.getString(s"$process.fields.split.psn.string")
    val orgSplit = conf.getString(s"$process.fields.split.org.string")


    val processTable = conf.getString(s"$process.database.tableName")

    val driverPath = conf.getString("database.driver.path")

    val databaseORGTable = conf.getString(s"$process.database.org.table.name")
    val databasePSNTable = conf.getString(s"$process.database.psn.table.name")

    val initedNeeded = conf.getBoolean(s"$process.database.init")

    val outputPSNTableName = conf.getString("database.output.person.name")
    val outputORGTableName = conf.getString("database.output.organization.name")
    val outputPSNPRORelTableName = conf.getString("database.output.relations.name.pro")
    val outputPSNORGRelTableName = conf.getString("database.output.relations.name.org")

    val strategy = conf.getString(s"$process.database.output.strategy")

    val beforeOrg = conf.getString(s"$process.fields.duplicates.before.org")
    val beforePSN = conf.getString(s"$process.fields.duplicates.before.name")

    val psnPSNName = conf.getString(s"$process.fields.duplicates.psn.name")
    val psnORGName = conf.getString(s"$process.fields.duplicates.psn.org")
    val orgName = conf.getString(s"$process.fields.duplicates.org.name")

    val PSNFieldMapping = Map(beforeOrg -> psnORGName, beforePSN -> psnPSNName)
    val ORGFieldMapping = Map(beforeOrg -> orgName)

    val needMapping = conf.getBoolean(s"$process.mapping.isMapping")

    val table = MySQLUtils.openTable(sparkSession,
      processTable)
    val schema = table.schema
    val rows = table.rdd.flatMap(r =>
      MySQLUtils
        .rowFlatDouble(r, schema.fieldIndex(duplicateFields.split(",")(0)),
          schema.fieldIndex(duplicateFields.split(",")(1)),
          psnSplit,
          orgSplit))
    //mapping part
    val rowAfterMapping = if (needMapping) {
      val mappingTableName = conf.getString(s"$process.mapping.table")
      val mappingBeforeField = conf.getString(s"$process.mapping.fields")
      val mappingAfterField = conf.getString(s"$process.mapping.tofields")
      val mappingTable = MySQLUtils.openTable(sparkSession, mappingTableName)
        .select(new Column(mappingBeforeField), new Column(mappingAfterField))
        .rdd
        .map(row => (row.getAs[String](mappingBeforeField), row.getAs[String](mappingAfterField)))
        .collect()
        .toMap
      val mapping = sparkSession.sparkContext.broadcast(mappingTable)
      rows.map(r => MySQLUtils.rowMappingValue(r, schema.fieldIndex(beforeOrg), mapping.value))
    } else rows
    //mapping end
    val df = sparkSession.createDataFrame(rowAfterMapping, schema)

    val uuid = utils.UUIDEvaluator.getInstance()
    //id build
    val originRDD = df.select(fields.split(",").map(s => new Column(s)) : _*)
      .rdd
      .map(r => MySQLUtils.rowBuildID(r, uuid))

    val saveSchema = new StructType(Array(StructField("ID", StringType, nullable = true)) ++
      fields.split(",").map(s => StructField(PSNFieldMapping.getOrElse(s, s.toUpperCase), StringType, nullable = true))
    )
    println("data schema is list below")
    saveSchema.foreach(f => print(f.name + "|"))
    println()

    val originTable = sparkSession.createDataFrame(originRDD, saveSchema)
    originTable.show(100)
    //    MySQLUtils.saveTable(originTable,outputPSNTableName, serverUrl
    //      ,driverType,user,passwd,driverName,SaveMode.Overwrite)
    //    originRDD.take(10).foreach(println(_))

    val initPSNDupTable = if(initedNeeded) MySQLUtils.openTable(sparkSession,
      databasePSNTable) else MySQLUtils.openTable(sparkSession,
      outputPSNTableName)

    val initORGDupTable = if(initedNeeded) MySQLUtils.openTable(sparkSession,
     databaseORGTable) else MySQLUtils.openTable(sparkSession,
     outputORGTableName)
    val orgSchema = initORGDupTable.schema
    println("org schema is list below")
    orgSchema.foreach(f => print(f.name + "|"))
    println()
    val (psnTable, proRelTable, orgRelTable) = MySQLUtils.rowMerge(sparkSession, initPSNDupTable,
      originTable, "CODE",
      "ID","ORG", psnPSNName, psnORGName)
    val orgDupSchemaOld = MySQLUtils.schemaMapping(orgRelTable.schema.add("ORGAN_ID", StringType, nullable = false), ORGFieldMapping)
    val orgDupTable = orgRelTable.rdd
      .map(MySQLUtils.rowBuildID(_, uuid))
      .map(MySQLUtils.buildRow(_, orgDupSchemaOld, orgSchema))
    val orgRDD = orgDupTable.union(initORGDupTable.rdd)

    val orgTable = sparkSession.createDataFrame(orgRDD, orgSchema).dropDuplicates(orgName)

    MySQLUtils.saveTable(psnTable,outputPSNTableName,SaveMode.Overwrite)
    MySQLUtils.saveTable(proRelTable,outputPSNPRORelTableName,SaveMode.Overwrite)
    MySQLUtils.saveTable(orgRelTable,outputPSNORGRelTableName,SaveMode.Overwrite)
    MySQLUtils.saveTable(orgTable,outputORGTableName,SaveMode.Overwrite)
  }
}
