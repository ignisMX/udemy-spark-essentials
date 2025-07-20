package lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sparkContext = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 10000000
  val numbersRDD = sparkContext.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStock(filename: String) = {
    val source = Source.fromFile(filename)
    val stockValues = source.getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

    source.close()
    stockValues
  }

  val stocksRDD = sparkContext.parallelize(readStock("src/main/scala/resources/data/stocks.csv"))

  // 2b -reading from files
  val stocksRDD2 = sparkContext.textFile("src/main/scala/resources/data/stocks.csv")

  // 3 - read from data frame
  val stockDataFrame = spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("src/main/scala/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDataSet = stockDataFrame.as[StockValue]
  val stockRDD3 = stocksDataSet.rdd

  // RDD -> Data Frame
  val numbersDataFrame = numbersRDD.toDF()

  // RDD -> Data Set
  val numbersDataSet = spark.createDataset(numbersRDD) // you get to keep type info

  // Transformations

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformations
  val msCount = msftRDD.count() // eager action

  // counting
  val companyDamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering
    .fromLessThan[StockValue](
      (elementOne: StockValue, elementTwo:StockValue) =>
        elementOne.price < elementTwo.price)

  val minMsft = msftRDD.min()

  // reduce
  numbersRDD.reduce(_ * _)

  // Grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // very expensive

  // Partitioning

  val repartitionedStockRDD = stocksRDD.repartition(30)
  repartitionedStockRDD
    .toDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/scala/resources/data/output/stock30")

  /*
  Repartitioning is EXPENSIVE. Involves Shuffling.
  Best practice: partition EARLY, then process that.
  Size of a partition 10-100MB.
 */

  // coalesce
  val coalescedRDD = repartitionedStockRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/scala/resources/data/output/stocks15")

}
