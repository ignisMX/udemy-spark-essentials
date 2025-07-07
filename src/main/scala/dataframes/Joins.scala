package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

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

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5433/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  val salariesDataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

  val managerDataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val titlesDataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.titles")
    .load()

  val employeesMaxSalariesDataFrame = employeesDataFrame
    .join(salariesDataFrame, "emp_no")
    .groupBy("emp_no", "first_name", "last_name")
    .agg(max("salary"))
    .orderBy(col("emp_no").desc)

  employeesMaxSalariesDataFrame.show()

  val neverManagerEmployeeDataFrame = employeesDataFrame.join(managerDataFrame, "emp_no", "left_anti").orderBy("emp_no")
  neverManagerEmployeeDataFrame.show()

  val mostRecentJobTitlesDataFrame = titlesDataFrame.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDataFrame = employeesDataFrame.join(salariesDataFrame, "emp_no")
    .groupBy("emp_no", "first_name", "last_name")
    .agg(max("salary").as("maxSalary"))
    .orderBy(col("maxSalary").desc)
    .limit(10)

  val bestPaidTitleDataFrame = mostRecentJobTitlesDataFrame.join(bestPaidEmployeesDataFrame,"emp_no")
  bestPaidTitleDataFrame.show()
}
