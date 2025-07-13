package typesdatasets

import org.apache.spark.sql.{Encoders, SparkSession, Dataset}

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
  def readDataFrame(filename:String) = spark.read
    .option("inferSchema","true")
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
}
