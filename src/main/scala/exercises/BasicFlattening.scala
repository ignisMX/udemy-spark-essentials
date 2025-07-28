package exercises

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions.{column}


object BasicFlattening extends App {

  val spark = SparkSession.builder()
    .appName("Flattening")
    .config("spark.master", "local")
    .getOrCreate();

  val inputBasicDataFrame = spark.read.option("multiline", "true").json("src/main/scala/resources/data/basic-input.json")

  val columnSeq = flattenSchemaDataFrame(inputBasicDataFrame)
  println(s"column Seq $columnSeq")

  val flattenedDataFrame = inputBasicDataFrame.select(columnSeq: _*)
  flattenedDataFrame.show(false)

  def flattenSchemaDataFrame(dataFrame: DataFrame): Seq[Column] = {
    def flatten(schema: StructType, prefixPath: String): Seq[Column] = {
      schema.fields.flatMap{ field =>
        val fieldName = if (!prefixPath.isEmpty) s"$prefixPath.${field.name}" else field.name
        field.dataType match{
          case structType: StructType =>
            flatten(structType, fieldName)
          case _ =>
            Seq(column(fieldName).as(fieldName.replace(".","-")))
        }
      }
    }

    flatten(dataFrame.schema, "")
  }

}
