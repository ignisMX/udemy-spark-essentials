package dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/scala/resources/data/cars.json")

  firstDF.show(false)
  firstDF.printSchema()

  // get rows
  println("getting rows ...")
  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType

  // schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType),
    )
  )

  // obtain a schema
  val carsDFSchema = firstDF.schema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/scala/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  manualCarsDF.printSchema()

  // note: DFs have schemas, rows do not

  // create DFs with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *   - make
   *   - model
   *   - screen dimension
   *   - camera megapixels
   *
   * 2) Read another file from the data/ folder, e.g. movies.json
   *   - print its schema
   *   - count the number of rows, call count()
   */

  val smartphones = Seq(
    ("Samsung", "Galaxy S25", 8, 50),
    ("Samsung", "A6", 7, 32),
    ("Apple", "iPhone 7", 7, 40),
  )

  val smartPhoneSchema = StructType(
    Array(
      StructField("make", StringType),
      StructField("model", StringType),
      StructField("screen_dimension", LongType),
      StructField("camera_megapixels", LongType)
    )
  )
  val smartphoneDataFrameFromSpark = spark.createDataFrame(smartphones);

  smartphoneDataFrameFromSpark.printSchema()
  smartphoneDataFrameFromSpark.show()

  val smartphoneDataFrameWithToDF = smartphones.toDF("brand","model","screen dimension", "camera megapixels")
  smartphoneDataFrameWithToDF.printSchema()
  smartphoneDataFrameWithToDF.show()

  val movies = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/scala/resources/data/movies.json")

  movies.printSchema()
  println(movies.count())
}
