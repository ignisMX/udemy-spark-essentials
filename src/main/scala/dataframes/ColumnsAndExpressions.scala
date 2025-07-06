package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columnd And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")
  println(firstColumn)

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  // various select method

  import spark.implicits._

  val carsDFProjection = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    col("Year"), // Scala Symbol, auto-converted to column
    $"Horsepower",
    expr("Origin")
  )

  carsDFProjection.show()

  // select with plain column names
  carsDF.select("Name", "Year").show()

  // Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  carsWithSelectExprWeightsDF.show()

  // Data frames processing

  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  carsWithKg3DF.show()

  //renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  carsWithColumnRenamed.show()

  //careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()

  // remove a column
  val carsDFDroppedColumn = carsWithColumnRenamed.drop("Cylinders", "Displacement")
  //This is a new data frame with columns removed
  carsDFDroppedColumn.show()
  //original dataframe does not suffer changes
  carsWithColumnRenamed.show()

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  println("Chain Filter ===")
  americanPowerfulCarsDF.show()
  americanPowerfulCarsDF2.show()
  americanPowerfulCarsDF3.show()

  // union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/scala/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have same schema

  println("Union ===")
  allCarsDF.show(false)

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  println("Distinct ===")
  allCountriesDF.show()

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/movies.json")

  val moviesWithTwoColumns = moviesDataFrame.select("Title", "Director")
  val moviesWithTwoColumnsWitColDataFrame = moviesDataFrame.select(col("Title"), column("Director"))
  moviesWithTwoColumns.show(false)
  moviesWithTwoColumnsWitColDataFrame.show(false)

  val moviesTotalProfitDataFrame = moviesDataFrame.select(
    col("Title"),
    column("US_Gross"),
    $"Worldwide_Gross",
    col("US_DVD_Sales"),
    expr("US_Gross + Worldwide_Gross").as("Total_Profit")
  )

  val moviesTotalProfitDataFrameExpr = moviesDataFrame.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross as Total_Gross"
  ).as("Total Profit")

  val totalProfitColumn = moviesDataFrame.col("US_Gross") + moviesDataFrame.col("Worldwide_Gross")
  val moviesTotalProfitDataFrameWithColumn = moviesDataFrame.select(
    $"Title",
    $"US_Gross",
    $"Worldwide_Gross",
    $"US_DVD_Sales",
    totalProfitColumn.as("Total Profit"))

  val moviesTotalProfitDataFrameWithColumnExpr = moviesDataFrame.select(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales")
    .withColumn("Total Gross", col("US_Gross") + col("Worldwide_Gross"))

  moviesTotalProfitDataFrame.show(false)
  moviesTotalProfitDataFrameExpr.show(false)
  moviesTotalProfitDataFrameWithColumn.show(false)
  moviesTotalProfitDataFrameWithColumnExpr.show(false)


  val comedyMoviesDataFrame = moviesDataFrame.select("Title","IMDB_Rating").filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  val comedyMoviesDataFrameWithFilterChain = moviesDataFrame.select("Title","IMDB_Rating").where(col("Major_Genre") === "Comedy").where(col("IMDB_Rating") > 6)
  comedyMoviesDataFrame.show(false)
  comedyMoviesDataFrameWithFilterChain.show(false)

}
