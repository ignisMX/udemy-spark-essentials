package exercises

import config.CurrentPrincipalsConfig
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{column, explode, expr}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object BasicArrayFlattening extends App {

  private val organization = "enriched.organization."
  private val duns = "duns"
  private val primaryName = "primaryName"
  private val countryISOAlpha2Code = "countryISOAlpha2Code"
  private val CURRENT_PRINCIPAL = "currentPrincipals"

  val spark = SparkSession.builder()
    .appName("Basic Array Flattening")
    .config("spark.master", "local")
    .getOrCreate();

  /*
  val inputDataFrame = spark.read.option("multiline", "true").json("src/main/scala/resources/data/input.json")
  inputDataFrame.show(false)
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
   */

  //-------------------------------------------------------------------------------------------

  println(s"----------------------------------------------------------------------------------")
  println("Current Principals")
  val currentPrincipals = spark.read.option("multiline", "true").json("src/main/scala/resources/data/current-principals-response.json")
  currentPrincipals.show(false)
  val currentPrincipalsDataFrame = createBaseDataFrame(currentPrincipals, CURRENT_PRINCIPAL,organization)
  currentPrincipalsDataFrame.show(false)
  val currentPrincipalsColumnSeq = flattenDataFrame(currentPrincipalsDataFrame)
  val basicArrayFlattenedCurrentPrincipalsDataFrame = currentPrincipalsDataFrame.select(currentPrincipalsColumnSeq: _*)
  basicArrayFlattenedCurrentPrincipalsDataFrame.show(false)
  val flattenedCurrentPrincipalsDataFrame = arrayFlatteningBasic(basicArrayFlattenedCurrentPrincipalsDataFrame, CurrentPrincipalsConfig.sortMetadata, CurrentPrincipalsConfig.metadata)
  flattenedCurrentPrincipalsDataFrame.show(false)


  def createBaseDataFrame(data: DataFrame, principals: String, key: String): DataFrame = {

    val expandedDfCols = data.select(organization.concat(duns), organization.concat(primaryName), organization.concat(countryISOAlpha2Code), key.concat(principals))
      .toDF(duns, primaryName, countryISOAlpha2Code, principals)

    val dataFrame = expandedDfCols.withColumn(principals,
        explode(column(principals)))
      .select(duns, primaryName, countryISOAlpha2Code, principals)

    dataFrame
  }

  def flattenDataFrame(dataFrame: DataFrame): Seq[Column] = {
    def flatten(schema: StructType, prefixPath: String): Seq[Column] = {
      schema.fields.flatMap { field =>
        val fieldName = if (!prefixPath.isEmpty) s"$prefixPath.${field.name}" else field.name
        field.dataType match {
          case structType: StructType =>
            flatten(structType, fieldName)
          case _ =>
            Seq(column(fieldName).as(fieldName.replace(".", "_")))
        }
      }
    }

    flatten(dataFrame.schema, "")
  }

  def arrayFlatteningBasic(dataFrame: DataFrame, sortMetadata: Map[String, Seq[(String, String)]], metadata: Map[String, Map[String, Map[Int, String]]]): DataFrame = {

    sortMetadata.foldLeft(dataFrame) { case (accumulatorDataFrame, (arrayColumnName, sortFields)) =>
      if(accumulatorDataFrame.columns.contains(arrayColumnName)) {
        val arrayType = isArrayType(accumulatorDataFrame.schema(arrayColumnName))

        if (!arrayType.elementType.isInstanceOf[StructType])
          return accumulatorDataFrame.drop(arrayColumnName)

        val sortExpression =
          s"""
             |array_sort(
             |  filter($arrayColumnName, element -> element IS NOT NULL),
             |  (left, right) -> (
             |    ${generateComparisonExpr("left", "right", sortFields)}
             |  )
             |)
        """.stripMargin

        println(sortExpression)
        val sortedColumnName = s"${arrayColumnName}_sorted"
        val sortedDataFrame = accumulatorDataFrame.withColumn(sortedColumnName, expr(sortExpression))

        val attributeNamePositionColumnName: Seq[(String, Int, String)] = metadata.getOrElse(arrayColumnName, Map.empty)
          .flatMap { case (field, positionNameMap) =>
            positionNameMap.map { case (index, flatColumnName) =>
              (field, index, flatColumnName)
            }
          }.toSeq

        println(attributeNamePositionColumnName)
        val arrayFlattenedDataFrame = attributeNamePositionColumnName.foldLeft(sortedDataFrame) {
          case (accumulatorFlattenedDataFrame, (attribute, position, flatColumnName)) =>
            val attributePath = attribute.split("_")
            val columnExpression = attributePath.foldLeft(column(sortedColumnName).getItem(position)){
              case (expressionAccumulator, property) => expressionAccumulator.getField(property)
            }
            accumulatorFlattenedDataFrame.withColumn(flatColumnName, columnExpression)
        }
        arrayFlattenedDataFrame.drop(sortedColumnName, arrayColumnName)
      }
      else accumulatorDataFrame
    }

  }

  def isArrayType(structField: StructField): ArrayType = {
    structField.dataType match {
      case arrayType: ArrayType => arrayType
      case _ => throw new IllegalArgumentException(s"${structField.name} is not an array type")
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
