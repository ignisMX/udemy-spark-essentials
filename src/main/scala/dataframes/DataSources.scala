package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

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
    .save("src/main/scala/resources/data/cars_duplicated.json")
}
