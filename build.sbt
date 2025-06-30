ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "3.5.0"
val postgresVersion = "42.6.0"
val log4jVersion = "2.20.0"

resolvers += Resolver.mavenCentral

lazy val root = (project in file("."))
  .settings(
    name := "udemy-spark-essentials",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      // logging
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      // postgres for DB connectivity
      "org.postgresql" % "postgresql" % postgresVersion
    )
  )



