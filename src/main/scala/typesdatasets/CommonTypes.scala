package typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/movies.json")

  // adding a plain value to a data frame
  val plainTextDataFrame = moviesDataFrame.select(col("Title"), lit(47).as("Plain Text"))
  plainTextDataFrame.show

  //Booleans
  val dramaFilter: Column = col("Major_Genre").equalTo("Drama")
  val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  val preferredFilter: Column = dramaFilter and goodRatingFilter

  val moviesDramaFilterDataFrame = moviesDataFrame.select("Title").where(dramaFilter)
  moviesDramaFilterDataFrame.show

  // + multiple ways of filtering
  val moviesWithGoodnessFlagsDataFrame = moviesDataFrame.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagsDataFrame.show()

  // filter on a boolean column
  val filteredMoviesWithGoodnessFlagsDataFrame = moviesWithGoodnessFlagsDataFrame.where("good_movie")
  filteredMoviesWithGoodnessFlagsDataFrame.show()

  // negations
  val filteredMoviesWithNotGoodnessFlagsDataFrame = moviesWithGoodnessFlagsDataFrame.where(not(col("good_movie")))
  filteredMoviesWithNotGoodnessFlagsDataFrame.show()

  // Numbers
  // math operators
  val moviesAvgRatingsDataFrame = moviesDataFrame.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  moviesAvgRatingsDataFrame.show()

  // correlation = number between -1 and 1
  println(moviesDataFrame.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) /*  corr is an ACTION */

  // Strings

  val carsDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  val carsInitCapDataFrame = carsDataFrame.select(initcap(col("Name")))
  carsInitCapDataFrame.show()

  // contains
  val volkswagenDataFrame = carsDataFrame.select("*").where(col("Name").contains("volkswagen"))
  volkswagenDataFrame.show()

  //regex
  val regexString = "volkswagen|vw"
  val vwDataFrame = carsDataFrame.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDataFrame.show()

  val vwReplaceDataFrame = vwDataFrame.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  vwReplaceDataFrame.show(false)

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")


  val carsRegExp = getCarNames.map(_.toLowerCase()).mkString("|")
  println(s"carsRegExp: $carsRegExp")
  val filteredCarsByRegExpDataFrame = carsDataFrame.select(
    col("Name"),
    regexp_extract(col("Name"), carsRegExp, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  filteredCarsByRegExpDataFrame.show(false)

  val carsFilters: List[Column] = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carsFilters.fold(lit(false))((accumulator, filter) => accumulator or filter)
  carsDataFrame.filter(bigFilter).show
}
