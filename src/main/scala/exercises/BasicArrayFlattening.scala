package exercises

import org.apache.spark.sql.functions.{ column, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object BasicArrayFlattening extends App {


  val spark = SparkSession.builder()
    .appName("Basic Array Flattening")
    .config("spark.master", "local")
    .getOrCreate();

  val inputDataFrame = spark.read.option("multiline", "true").json("src/main/scala/resources/data/input.json")
  val columnSeq = flattenDataFrame(inputDataFrame)
  val basicArrayFlattenedDataFrame = inputDataFrame.select(columnSeq: _*)
  basicArrayFlattenedDataFrame.show(false)

  val sortMetadata: Map[String, Seq[(String, String)]] = Map.apply(
    "addresses" -> Seq("type" -> "ASC", "zip" -> "DESC"))

  val metadata: Map[String, Map[String, Map[Int, String]]] =
    Map("addresses" -> Map(
      "type" -> Map(
        0 -> "Type_1",
        1 -> "Type_2",
        2 -> "Type_3",
      ),
      "street" -> Map(
        0 -> "Street_1",
        1 -> "Street_2",
        2 -> "Street_3",
      ),
      "city" -> Map(
        0 -> "City_1",
        1 -> "City_2",
        2 -> "City_3",
      ),
      "zip" -> Map(
        0 -> "Zip_1",
        1 -> "Zip_2",
        2 -> "Zip_3",
      )
    ))

  val flattendBasicArrayDataFrame = arrayFlatteningBasic(basicArrayFlattenedDataFrame, sortMetadata, metadata)
  println(s"flattened DataFrame ---------------------------------------------")
  flattendBasicArrayDataFrame.show(false)

  def flattenDataFrame(dataFrame: DataFrame): Seq[Column] = {
    def flatten(schema: StructType, prefixPath: String): Seq[Column] = {
      schema.fields.flatMap { field =>
        val fieldName = if (!prefixPath.isEmpty) s"$prefixPath.${field.name}" else field.name
        field.dataType match {
          case structType: StructType =>
            flatten(structType, fieldName)
          case _ =>
            Seq(column(fieldName).as(fieldName.replace(".", "-")))
        }
      }
    }

    flatten(dataFrame.schema, "")
  }

  def arrayFlatteningBasic(dataFrame: DataFrame, sortMetadata: Map[String, Seq[(String, String)]], metadata: Map[String, Map[String, Map[Int, String]]]): DataFrame = {

    sortMetadata.foldLeft(dataFrame) { case (accumulatorDataFrame, (arrayColumnName, sortFields)) =>
      println(s"arrayColName: $arrayColumnName")
      println(s"sortFields: $sortFields")
      println(s"schema of objects inside of array: ${accumulatorDataFrame.schema(arrayColumnName)}")
      val arrayType = isArrayType(accumulatorDataFrame.schema(arrayColumnName))
      println(s"arrayType: $arrayType")
      val structType = getStructTypeOfArrayElements(arrayType, arrayColumnName)
      println(s"structType: $structType")

      val sortExpression =
        s"""
           |array_sort(
           |  filter($arrayColumnName, element -> element IS NOT NULL),
           |  (left, right) -> (
           |    ${generateComparisonExpr("left", "right", sortFields)}
           |  )
           |)
        """.stripMargin

      println(s"sortExpression: $sortExpression")

      val sortedColumnName = s"${arrayColumnName}_sorted"
      val sortedDataFrame = accumulatorDataFrame.withColumn(sortedColumnName, expr(sortExpression))

      val attributeNamePositionColumnName: Seq[(String, Int, String)] = metadata.getOrElse(arrayColumnName, Map.empty)
        .flatMap { case (field, positionNameMap) =>
          positionNameMap.map { case (index, flatColumnName) =>
            (field, index, flatColumnName)
          }
        }.toSeq

      println(s"fieldNamePositionColumnName $attributeNamePositionColumnName")

      val arrayFlattenedDataFrame = attributeNamePositionColumnName.foldLeft(sortedDataFrame) {
        case (accumulatorFlattenedDataFrame, (attribute, position, flatColumnName)) =>
          accumulatorFlattenedDataFrame.withColumn(flatColumnName, column(sortedColumnName).getItem(position).getField(attribute))
      }

      arrayFlattenedDataFrame.show(false)

      arrayFlattenedDataFrame.drop(sortedColumnName, arrayColumnName)
    }

  }

  def isArrayType(structField: StructField): ArrayType = {
    structField.dataType match {
      case arrayType: ArrayType => arrayType
      case _ => throw new IllegalArgumentException(s"${structField.name} is not an array type")
    }
  }

  def getStructTypeOfArrayElements(arrayField: ArrayType, arrayFieldName: String): StructType = {
    arrayField.elementType match {
      case structType: StructType => structType
      case anotherType => throw new IllegalArgumentException(s"The elements into $arrayFieldName are not StructType: They are $anotherType")
    }
  }

  def generateComparisonExpr(leftVar: String, rightVar: String, fields: Seq[(String, String)]): String = {
    fields.map { case (field, direction) =>
      val comparisonOrder = s"CASE WHEN $leftVar.$field <=> $rightVar.$field THEN 0 " +
        s"WHEN $leftVar.$field IS NULL THEN -1 " +
        s"WHEN $rightVar.$field IS NULL THEN 1 " +
        s"WHEN $leftVar.$field < $rightVar.$field THEN -1 " +
        s"ELSE 1 END"

      direction.toUpperCase match {
        case "ASC" => comparisonOrder
        case "DESC" => s"-$comparisonOrder"
        case _ => comparisonOrder
      }
    }.mkString(" + ")
  }


}
