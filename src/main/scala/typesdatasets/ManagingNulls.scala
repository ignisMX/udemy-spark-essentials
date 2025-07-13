package typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDataFrame = spark.read
    .option("inferSchema","true")
    .json("src/main/scala/resources/data/movies.json")

  // select the first non-null value

  val moviesFirstNonNullValueDataFrame = moviesDataFrame
    .select(
      column("Title"),
      column("Rotten_Tomatoes_Rating"),
      column("IMDB_Rating"),
      coalesce(column("Rotten_Tomatoes_Rating"), column("IMDB_Rating") * 10)
    )

  moviesFirstNonNullValueDataFrame.show(false)

  // checking for nulls
  val moviesCheckingNullsDataFrame =  moviesDataFrame.select("*").where(column("Rotten_Tomatoes_Rating").isNull)
  moviesCheckingNullsDataFrame.show(false)

  // nulls when ordering
  val moviesNullOrderingDataFrame = moviesDataFrame.orderBy(column("IMDB_Rating").desc_nulls_last)
  moviesNullOrderingDataFrame.show(false)

  // removing nulls
  val moviesNullsRemovedDataFrame = moviesDataFrame.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls
  moviesNullsRemovedDataFrame.show(false)

  // replace nulls
  val moviesFilledDataFrame =  moviesDataFrame.na.fill(0, List("IMDB_Rating","Rotten_Tomatoes_Rating"))
  moviesFilledDataFrame.show(false)

  val moviesFilledWithMapDataFrame = moviesDataFrame.na.fill(
    Map(
      "IMDB_Rating"->0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    )
  )

  moviesFilledWithMapDataFrame.show(false)

  // complex operations
  val moviesComplexOperationsDataFrame = moviesDataFrame.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as ifNull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as nullIf", // returns null if the two values are EQUAL, ELSE first value
    "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating * 10, 0.0) as nvl2" // if(first != null) second else third
  )

  moviesComplexOperationsDataFrame.show(false)


  println(null == 5)
  println(5 == null)
}
