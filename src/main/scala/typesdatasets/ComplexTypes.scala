package typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/movies.json")

  // Dates

  val moviesWithReleaseDatesDataFrame = moviesDataFrame
    .select(column("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_release"))

  moviesWithReleaseDatesDataFrame.show(false)

  moviesWithReleaseDatesDataFrame
    .withColumn("Today", curdate()) //today
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_age", datediff(col("Today"), col("Actual_Release")) / 365).show(false)

  moviesWithReleaseDatesDataFrame.select("*").where(column("Actual_release").isNull).show(false)

  /**
   * Exercise
   * 1. How do we deal with multiple date formats?
   * 2. Read the stocks DF and parse the dates
   */

  val stocksDataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/scala/resources/data/stocks.csv")

  val stockDateFormattedDataFrame = stocksDataFrame.select(column("symbol"), column("price"), to_date(column("date"), "MMM dd yyyy").as("dateFormatted"))
  stockDateFormattedDataFrame.show(false)

  // structures

  // 1 - with col operators
  val moviesNestedObjectsDataFrame = moviesDataFrame
    .select(col("Title"), struct(col("US_Gross"), column("Worldwide_Gross")).as("Profit"))

  moviesNestedObjectsDataFrame.show(false)

  val moviesExtractedFieldDataFrame = moviesNestedObjectsDataFrame.select(column("Title"), column("Profit").getField("US_Gross").as("US_Profit"))
  moviesExtractedFieldDataFrame.show(false)

  // 2 - with expression strings
  val moviesNestedObjectsStringExpressionDataFrame = moviesDataFrame.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit_String_Expression")
  moviesNestedObjectsStringExpressionDataFrame.show(false)

  val moviesExtractedFieldStringExpressionDataFrame = moviesNestedObjectsStringExpressionDataFrame.selectExpr("Title", "Profit_String_Expression.US_Gross")
  moviesExtractedFieldStringExpressionDataFrame.show(false)

  // Arrays
  val moviesWithWords = moviesDataFrame.select(
    column("Title"),
    split(column("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.show(false)

  val moviesArrayExpressionDataFrame = moviesWithWords
    .select(
      column("Title"),
      column("Title_Words"),
      expr("Title_Words[0]"),
      size(column("Title_Words")),
      array_contains(column("Title_Words"), "Love")
    )

  moviesArrayExpressionDataFrame.show(false)

  moviesWithWords
    .selectExpr(
      "Title",
      "Title_Words",
      "Title_Words[0]",
      "size(Title_Words)",
      "array_contains(Title_Words, 'Love') as Contains_Love"
    ).show(false)
}
