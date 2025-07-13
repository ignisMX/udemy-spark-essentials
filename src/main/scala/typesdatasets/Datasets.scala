package typesdatasets

import org.apache.spark.sql.{Encoders, SparkSession, Dataset}
import org.apache.spark.sql.functions._

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("DataSets")
    .config("spark.master", "local")
    .getOrCreate()

  val numberDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/scala/resources/data/numbers.csv")

  numberDataFrame.printSchema()

  //convert data frame into data set
  implicit val intEncoder = Encoders.scalaInt
  val numberDataSet: Dataset[Int] = numberDataFrame.as[Int]

  // dataset of complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2 - read the data frame from the file
  def readDataFrame(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/scala/resources/data/$filename")

  val carsDataFrame = readDataFrame("cars.json")

  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  var carsDataset = carsDataFrame.as[Car]
  carsDataset.show(false)

  // Dataset collection functions
  numberDataSet.filter(_ < 100)

  // map, flatMap, fold. reduce, for comprehensions ...
  val carNamesDataset = carsDataset.map(car => car.Name.toUpperCase())
  carNamesDataset.show(false)

  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

  val numberOfCars = carsDataset.count()
  println(s"number of cars: $numberOfCars")

  val numberOfPowerfulCars = carsDataset.filter(car => car.Horsepower.getOrElse(0L) > 140).count()
  println(s"number of powerful cars: $numberOfPowerfulCars")

  val sumOfHorsepower: Long = carsDataset.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)
  val averageHorsepower: Long = sumOfHorsepower / carsDataset.count()
  println(s"avergae horsepower $averageHorsepower")

  // data frames function
  val averageHorsepowerWithDataFrameFunction = carsDataset.select(avg(column("Horsepower")))
  averageHorsepowerWithDataFrameFunction.show()

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDataset = readDataFrame("guitars.json").as[Guitar]
  val guitarPlayerDataset = readDataFrame("guitarPlayer.json").as[GuitarPlayer]
  val bandsDataset = readDataFrame("bands.json").as[Band]

  val guitarPlayerBandsDataset : Dataset[(GuitarPlayer, Band)] = guitarPlayerDataset
    .joinWith(bandsDataset, guitarPlayerDataset.col("band") === bandsDataset.col("id"), "inner")

  guitarPlayerBandsDataset.show(false)


}
