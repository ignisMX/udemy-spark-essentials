package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDataFrame = spark.read
    .option("inferSchema","true")
    .json("src/main/scala/resources/data/movies.json")

  // counting values except nulls
  val genresCountDataFrame = moviesDataFrame.select(count(col("Major_Genre")))
  genresCountDataFrame.show();

  val genresCountExprDataFrame = moviesDataFrame.selectExpr("count(Major_Genre)")
  genresCountExprDataFrame.show()

  // counting all including nulls
  val genresCountWithNullsDataFrame = moviesDataFrame.select(count("*"))
  genresCountWithNullsDataFrame.show()

  // counting distinct, it does not include nulls
  val genreMoviesDistinctDataFrame = moviesDataFrame.select(countDistinct(col("Major_Genre")))
  genreMoviesDistinctDataFrame.show()

  // this does not return data frame just the value
  val numberOfDistinctValues = moviesDataFrame.select("Major_Genre").distinct().count()
  println(numberOfDistinctValues)

  // approximate count
  moviesDataFrame.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDataFrame = moviesDataFrame.select(min(col("IMDB_Rating")))
  minRatingDataFrame.show(false)

  val minRatingWithExprDataFrame = moviesDataFrame.selectExpr("min(IMDB_Rating)")
  minRatingWithExprDataFrame.show()

  // sum
  val sumUSGrossDataFrame = moviesDataFrame.select(sum(col("US_Gross")).as("Sum Gross"))
  sumUSGrossDataFrame.show()

  val sumUSGrossWithExprDataFrame = moviesDataFrame.selectExpr("sum(US_Gross) as Gross_Total")
  sumUSGrossWithExprDataFrame.show()

  // avg
  val rottenTomatoesAvg = moviesDataFrame.select(avg(col("Rotten_Tomatoes_Rating")).as("Rotten_Tomatoes_Rating_Avg"))
  rottenTomatoesAvg.show(false)

  val rottenTomatoesAvgExprDataFrame = moviesDataFrame.selectExpr("avg(Rotten_Tomatoes_Rating) as Rotten_Tomatoes_Rating_Avg")
  rottenTomatoesAvgExprDataFrame.show()

  // data science
  val dataScienceDataFrame = moviesDataFrame.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )
  dataScienceDataFrame.show(false)

  // Grouping
  val countByGenreDataFrame = moviesDataFrame
    .groupBy(col("Major_Genre")) // Including null
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDataFrame.show()

  val avgRatingByGenreDataFrame = moviesDataFrame
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  avgRatingByGenreDataFrame.show(false)

  val aggregationByGenreDataFrame = moviesDataFrame.
    groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationByGenreDataFrame.show()

}
