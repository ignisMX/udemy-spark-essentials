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
  ), true)

  // read data frame loaded Spark tables
  val employeesDataFrame = spark.read.table("employees")
  employeesDataFrame.show(false)

  /**
   * Exercises
   *
   * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
   * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   */


  val moviesDataFrame = spark.read
    .option("inferSchema","true")
    .json("src/main/scala/resources/data/movies.json")

  moviesDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("movies")
  val moviesTableDataFrame = spark.read.table("movies")
  moviesTableDataFrame.show(false)

  val countedEmployeesDataFrame = spark.sql(
    """
      |SELECT COUNT(*) hired FROM employees WHERE hire_date BETWEEN '2000-01-01' AND '2001-01-01'
      |""".stripMargin)

  countedEmployeesDataFrame.show(false)

  val averageSalaryByDepartmentDataFrame = spark.sql(
    """
      |SELECT AVG(salaries.salary) avg_salary, dept_emp.dept_no department_number FROM employees
      |INNER JOIN salaries ON salaries.emp_no = employees.emp_no
      |INNER JOIN dept_emp ON employees.emp_no = dept_emp.emp_no
      |WHERE employees.hire_date BETWEEN '1990-01-01' AND '2001-01-01'
      |GROUP BY dept_emp.dept_no
      |""".stripMargin)

  averageSalaryByDepartmentDataFrame.show(false)

  val bestPaidDepartment = spark.sql(
    """
      |SELECT AVG(salaries.salary) avg_salary, dept_emp.dept_no department_number, departments.dept_name FROM employees
      |INNER JOIN salaries ON salaries.emp_no = employees.emp_no
      |INNER JOIN dept_emp ON employees.emp_no = dept_emp.emp_no
      |INNER JOIN departments ON departments.dept_no = dept_emp.dept_no
      |WHERE employees.hire_date BETWEEN '1990-01-01' AND '2001-01-01'
      |GROUP BY dept_emp.dept_no, departments.dept_name
      |ORDER BY avg_salary DESC
      |LIMIT 1
      |""".stripMargin)

  bestPaidDepartment.show(false)
}
