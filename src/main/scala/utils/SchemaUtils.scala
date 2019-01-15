package utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

object SchemaUtils {
  def SchemaIntersection(schemas : StructType *) : StructType = {
    schemas.reduce((a, b) => {
      val setB = b.fields.map(f => f.name).toSet
      var seq = Seq[StructField]()
      for (a_ <- a) {
        if (setB.contains(a_.name)) {
          seq = seq :+ a_
        }
      }
      StructType(seq)
    })
  }

  /**
    * find all the field in B but not in A
    *
    * @param schemaA
    * @param schemaB
    * @return
    */
  def SchemaExclusive (schemaA : StructType , schemaB : StructType) : StructType = {
    val setA = schemaA.fields.map(f => f.name).toSet
    var seq = Seq[StructField]()
    for (b_ <- schemaB) {
      if (!setA.contains(b_.name)) {
        seq = seq :+ b_
      }
    }
    StructType(seq)
  }

  def SchemaUnion (schemas : StructType *) : StructType = {
    val map : mutable.HashMap[String, StructField] = new mutable.HashMap[String, StructField]()
    schemas
      .map(struct => {
        struct.fields.map(s => (s.name, s))
      }).foreach(
      pairs => {
        pairs.foreach(
          pair => {
            map.put(pair._1, pair._2)
          }
        )
      }
    )
    new StructType(map.values.toArray)
  }
  // test all pass
  def main(args: Array[String]): Unit = {
    val schemaA = StructType(Array(StructField("t1", StringType),StructField("t2", StringType),StructField("t3", StringType)))
    val schemaB = StructType(Array(StructField("d1", StringType),StructField("t2", StringType),StructField("t3", StringType)))
    println(SchemaUnion(schemaA, schemaB)) //StructType(StructField(t1,StringType,true), StructField(t3,StringType,true), StructField(t2,StringType,true), StructField(d1,StringType,true))
    println(SchemaExclusive(schemaA, schemaB)) //StructType(StructField(d1,StringType,true))
    println(SchemaIntersection(schemaA, schemaB)) //StructType(StructField(t2,StringType,true), StructField(t3,StringType,true))
  }
}
