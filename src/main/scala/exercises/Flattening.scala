package exercises

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions.{column}


object Flattening extends App {

  val spark = SparkSession.builder()
    .appName("Flattening")
    .config("spark.master", "local")
    .getOrCreate();

  val inputDataFrame = spark.read.option("multiline", "true").json("src/main/scala/resources/data/input.json")
  val inputBasicDataFrame = spark.read.option("multiline", "true").json("src/main/scala/resources/data/basic-input.json")

  val flattenedSchemaWithPath = flattenSchema(inputBasicDataFrame.schema.fields, "")
  println(s"flattened schema with path ${flattenedSchemaWithPath.toSeq}")

  val columnSeq = flattenSchemaDataFrame(inputBasicDataFrame)
  println(s"column Seq $columnSeq")
  val flattenedDataFrame = inputBasicDataFrame.select(columnSeq: _*)
  flattenedDataFrame.show(false)

  def flattenSchema(fields: Array[StructField], prefixPath: String): Array[StructField] = {
    fields.flatMap { field =>
      field.dataType match {
        case structType: StructType => flattenSchema(structType.fields, field.name)
        case _ => {
          val fieldName = if (!prefixPath.isEmpty) s"${prefixPath}-${field.name}" else field.name
          Array(StructField(fieldName, field.dataType, field.nullable))
        }
      }
    }
  }

  def flattenSchemaDataFrame(dataFrame: DataFrame): Seq[Column] = {
    def flatten(schema: StructType, prefixPath: String): Seq[Column] = {
      schema.fields.flatMap{ field =>
        field.dataType match{
          case structType: StructType => flatten(structType, field.name)
          case _ =>
            val fieldName = if (!prefixPath.isEmpty) s"$prefixPath.${field.name}" else field.name
            Seq(column(fieldName).as(fieldName.replace(".","-")))
        }
      }
    }

    flatten(dataFrame.schema, "")
  }

  //"+1-555-0202"
}
