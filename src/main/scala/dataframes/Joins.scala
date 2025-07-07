package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/guitars.json")

  val guitaristDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/guitarPlayer.json")

  val bandsDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristDataFrame.col("band") === bandsDataFrame.col("id")
  val guitaristsBandDataFrame = guitaristDataFrame.join(bandsDataFrame, joinCondition, "inner")

  guitaristsBandDataFrame.show(false)

  // outer
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  val leftOuterJoin = guitaristDataFrame.join(bandsDataFrame, joinCondition, "left_outer")

  leftOuterJoin.show()

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  val rightOuterJoin = guitaristDataFrame.join(bandsDataFrame, joinCondition, "right_outer")

  rightOuterJoin.show()

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  val outerJoin = guitaristDataFrame.join(bandsDataFrame, joinCondition, "outer")

  outerJoin.show()

  // semi-joins = everything in the left data frame for which there is a row in the right data frame satisfying the condition
  val leftSemiJoin = guitaristDataFrame.join(bandsDataFrame, joinCondition, "left_semi")

  leftSemiJoin.show()

  // anti-joins = everything in the left data frame for which there is NO row in the right data frame satisfying the condition
  val antiJoin = guitaristDataFrame.join(bandsDataFrame, joinCondition, "left_anti")

  antiJoin.show()

  // things to bear in mind
  // guitaristsBandDataFrame.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  val joinRenamedColumnDataFrame = guitaristDataFrame.join(bandsDataFrame.withColumnRenamed("id", "band"), "band")

  joinRenamedColumnDataFrame.show()

  // option 2 - drop the dupe column
  guitaristsBandDataFrame.drop(bandsDataFrame.col("id")).show()

  // option 3 - rename the offending column and keep the data
  val bandsWithColumnRenamedDataFrame = bandsDataFrame.withColumnRenamed("id", "bandId")

  guitaristDataFrame.
    join(bandsWithColumnRenamedDataFrame, guitaristDataFrame.col("band") === bandsWithColumnRenamedDataFrame.col("bandId"))
    .show()

  // using complex types
  guitaristDataFrame.join(guitarsDataFrame.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show(false)


}
