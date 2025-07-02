package dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Datat sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /*
    Reading a DF:
      - format
      - schema or inferSchema = true
      - path
      - zero or more options
   */

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path", "src/main/scala/resources/data/cars.json")
    .load()

  carsDF.show()

  // alternative reading with options map
  val carsDFWithOptionsMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/scala/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  carsDFWithOptionsMap.show()

  /*
    Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/scala/resources/data/output/cars_duplicated.json")

  // JSON Flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip, gzip, lz4, snappy, deflated
    .json("src/main/scala/resources/data/cars.json")

  // CSV Flags
  val stockSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/scala/resources/data/stocks.csv").show()

  // parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/scala/resources/data/output/cars.parquet")

  // Text files
  spark.read.text("src/main/scala/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5433/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val movies = spark.read
    .format("json")
    .load("src/main/scala/resources/data/movies.json")

  movies.show()

  movies.write
    .mode(SaveMode.Overwrite)
    .option("sep","\t")
    .option("header","true")
    .csv("src/main/scala/resources/data/output/movies-tab-separator.csv")

  movies.write
    .mode(SaveMode.Overwrite)
    .option("sep","  ")
    .option("compression","snappy")
    .parquet("src/main/scala/resources/data/output/movies-parquet-snappy.csv")

  movies.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
