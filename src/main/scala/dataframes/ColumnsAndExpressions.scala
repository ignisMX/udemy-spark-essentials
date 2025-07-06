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
  val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/scala/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have same schema

  println("Union ===")
  allCarsDF.show(false)

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  println("Distinct ===")
  allCountriesDF.show()


}
