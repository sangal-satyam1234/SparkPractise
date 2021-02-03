package assignment

import assignment.Driver.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import utility.JsonFlatMapper

import java.net.URL
import scala.collection.JavaConverters.mapAsScalaMapConverter

object Question2 {

  def process(url: URL) = {
    val df = spark.read.json(url.getFile)
    println("First 10 entries")
    df.show(10)

    // Make 2 columns,1 with original json and other with expanded json
    val df2 = df.rdd.map(entry => {
      val flatJson = JsonFlatMapper.flatMap(entry.getString(0))
      (entry.getString(0), javaToScalaMap(flatJson))
    })

    // Determine schema for each entry
    val df3 = df2.map {
      map =>
        val structFields = map._2.map(e => entryToStructField(e._1, e._2))
        new StructType(structFields.toArray)
    }

    //Infer final schema from all schema
    val init = new StructType().add(StructField("l1", DataTypes.StringType, false))
    val schema = df3.fold(init)((left, right) => StructType((left ++: right).distinct))

    //Infer a RDD[Row]
    val df4: RDD[Row] = df2.map(mapToRow(_, schema))
    println(df4.collect().mkString("\n"))

    //Infer dataset[row] with final schema
    val DF2 = spark.createDataFrame(df4, schema)
    DF2.show(10)
  }

  // Utility for a deep map conversion
  def javaToScalaMap(map: java.util.Map[String, Object]): Map[String, Any] = {
    val scalaMap = map.asScala
    scalaMap.map {
      case (key, value) => (key, javaValueToScala(value))
    }.toMap
  }

  // Utility for unboxing java values
  def javaValueToScala(value: Any): Any = value match {
    case v: java.lang.Integer => Int.unbox(v)
    case v: java.lang.Long => Long.unbox(v)
    case v: java.lang.Float => Float.unbox(v)
    case v: java.lang.Double => Double.unbox(v)
    case v: java.lang.Boolean => Boolean.unbox(v)
    case v => s"${v.toString}"
  }

  //Infer DataType for a value
  def valueToDataType(value: Any): DataType = value match {
    case _: Integer | Int => DataTypes.IntegerType
    case Long | _: java.lang.Long => DataTypes.LongType
    case Float | _: java.lang.Float => DataTypes.FloatType
    case Double | _: java.lang.Double => DataTypes.DoubleType
    case Boolean | _: java.lang.Boolean => DataTypes.BooleanType
    case _ => DataTypes.StringType
  }

  //Infer default values for a datatype
  def dataTypeToValue(dataType: DataType) = dataType match {
    case DataTypes.IntegerType => 0
    case DataTypes.LongType => 0L
    case DataTypes.FloatType => 0.0
    case DataTypes.DoubleType => 0.0
    case DataTypes.BooleanType => false
    case DataTypes.StringType => ""
  }

  def entryToStructField(key: String, value: Any): StructField = {
    StructField(name = key, dataType = valueToDataType(value), nullable = true)
  }

  def mapToRow(map: (String, Map[String, Any]), schema: StructType): org.apache.spark.sql.Row = {
    val row = schema.map(sf => {
      val colName = sf.name
      if ("l1".equals(colName)) map._1
      else {
        val colValue = map._2.getOrElse(colName, {
          dataTypeToValue(sf.dataType)
        })
        colValue
      }
    })
    Row.fromSeq(row.toSeq)
  }

}
