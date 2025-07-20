package sparksql

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark Sql Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir","src/main/scala/resources/warehouses")
    .getOrCreate()

  val carsDataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/scala/resources/data/cars.json")

  // regular API
  val originCarsDataFrame = carsDataFrame.select(column("Name")).where(col("Origin") === "USA")
  originCarsDataFrame.show(false)

  // use Spark SQL
  carsDataFrame.createOrReplaceTempView("cars")

  val americanCarsDataFrame = spark.sql(
    """
      |SELECT Name FROM cars WHERE Origin = 'USA'
    """.stripMargin)

  americanCarsDataFrame.show(false)

  // we can run ANY SQL statement
  spark.sql("CREATE database rtjvm")
  spark.sql("use rtjvm")

  val databaseDataFrame = spark.sql("SHOW DATABASES")
  databaseDataFrame.show(false)

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5433/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach{ tableName =>
    val tableDataFrame = readTable(tableName)
    tableDataFrame.createOrReplaceTempView(tableName)

    if(shouldWriteToWarehouse){
      tableDataFrame.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"
  ))

  // read data frame loaded Spark tables
  val employeesDataFrame = spark.read.table("employees")
  employeesDataFrame.show(false)
}
