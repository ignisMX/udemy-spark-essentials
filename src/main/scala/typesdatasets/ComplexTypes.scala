package typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master","local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDataFrame = spark.read
    .option("inferSchema","true")
    .json("src/main/scala/resources/data/movies.json")

  // Dates

  val moviesWithReleaseDatesDataFrame = moviesDataFrame
    .select(column("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_release"))

  moviesWithReleaseDatesDataFrame.show(false)

  moviesWithReleaseDatesDataFrame
    .withColumn("Today", curdate()) //today
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_age", datediff(col("Today"), col("Actual_Release")) / 365).show(false)

  moviesWithReleaseDatesDataFrame.select("*").where(column("Actual_release").isNull).show(false)


}
